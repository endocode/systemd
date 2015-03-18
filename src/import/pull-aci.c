/*-*- Mode: C; c-basic-offset: 8; indent-tabs-mode: nil -*-*/

/***
  This file is part of systemd.

  Copyright 2014-2015 Lennart Poettering
  Copyright 2015 Endocode AG

  systemd is free software; you can redistribute it and/or modify it
  under the terms of the GNU Lesser General Public License as published by
  the Free Software Foundation; either version 2.1 of the License, or
  (at your option) any later version.

  systemd is distributed in the hope that it will be useful, but
  WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General Public License
  along with systemd; If not, see <http://www.gnu.org/licenses/>.
***/

#include <ctype.h>
#include <curl/curl.h>
#include <sys/prctl.h>
#include <sys/utsname.h>

#include "architecture.h"
#include "sd-daemon.h"
#include "json.h"
#include "strv.h"
#include "btrfs-util.h"
#include "utf8.h"
#include "mkdir.h"
#include "path-util.h"
#include "import-util.h"
#include "curl-util.h"
#include "aufs-util.h"
#include "pull-job.h"
#include "pull-common.h"
#include "import-common.h"
#include "pull-aci.h"

typedef enum AciProgress {
        ACI_SIMPLE_DISCOVERY,
        ACI_META_DISCOVERY,
        ACI_DOWNLOADING,
        ACI_COPYING,
} AciProgress;

struct AciPull {
        sd_event *event;
        CurlGlue *glue;

        char *image_root;

        PullJob *simple_discovery_job;
        PullJob *meta_discovery_job;
        PullJob *download_job;

        char *name;
        char *tag;
        char *id;

        AciPullFinished on_finished;
        void *userdata;

        char *local;
        bool force_local;
        bool grow_machine_directory;

        char *temp_path;
        char *final_path;

        pid_t tar_pid;
};

#define PROTOCOL_PREFIX "https://"

static void aci_pull_job_on_finished(PullJob *j);

AciPull* aci_pull_unref(AciPull *i) {
        if (!i)
                return NULL;

        if (i->tar_pid > 1) {
                (void) kill_and_sigcont(i->tar_pid, SIGKILL);
                (void) wait_for_terminate(i->tar_pid, NULL);
        }

        pull_job_unref(i->simple_discovery_job);
        pull_job_unref(i->meta_discovery_job);
        pull_job_unref(i->download_job);

        curl_glue_unref(i->glue);
        sd_event_unref(i->event);

        if (i->temp_path) {
                (void) btrfs_subvol_remove(i->temp_path);
                (void) rm_rf_dangerous(i->temp_path, false, true, false);
                free(i->temp_path);
        }

        free(i->name);
        free(i->tag);
        free(i->id);
        free(i->final_path);
        free(i->image_root);
        free(i->local);
        free(i);

        return NULL;
}

int aci_pull_new(
                AciPull **ret,
                sd_event *event,
                const char *image_root,
                AciPullFinished on_finished,
                void *userdata) {

        _cleanup_(aci_pull_unrefp) AciPull *i = NULL;
        int r;

        assert(ret);

        i = new0(AciPull, 1);
        if (!i)
                return -ENOMEM;

        i->on_finished = on_finished;
        i->userdata = userdata;

        i->image_root = strdup(image_root ?: "/var/lib/machines");
        if (!i->image_root)
                return -ENOMEM;

        i->grow_machine_directory = path_startswith(i->image_root, "/var/lib/machines");

        if (event)
                i->event = sd_event_ref(event);
        else {
                r = sd_event_default(&i->event);
                if (r < 0)
                        return r;
        }

        r = curl_glue_new(&i->glue, i->event);
        if (r < 0)
                return r;

        i->glue->on_finished = pull_job_curl_on_finished;
        i->glue->userdata = i;

        *ret = i;
        i = NULL;

        return 0;
}

static void aci_pull_report_progress(AciPull *i, AciProgress p) {
        unsigned percent;

        assert(i);

        switch (p) {

        case ACI_SIMPLE_DISCOVERY:
                percent = 0;
                if (i->simple_discovery_job)
                        percent += i->simple_discovery_job->progress_percent * 50 / 100;
                break;

        case ACI_META_DISCOVERY:
                percent = 50;
                if (i->meta_discovery_job)
                        percent += i->meta_discovery_job->progress_percent * 5 / 100;
                break;

        case ACI_DOWNLOADING:
                percent = 55;
                if (i->download_job)
                        percent += i->download_job->progress_percent * 40 / 100;

                break;

        case ACI_COPYING:
                percent = 95;
                break;

        default:
                assert_not_reached("Unknown progress state");
        }

        sd_notifyf(false, "X_IMPORT_PROGRESS=%u", percent);
        log_debug("Combined progress %u%%", percent);
}

static int aci_pull_make_local_copy(AciPull *i) {
        int r;

        assert(i);

        if (!i->local)
                return 0;

        if (!i->final_path) {
                i->final_path = strjoin(i->image_root, "/.aci-", i->id, NULL);
                if (!i->final_path)
                        return log_oom();
        }

        r = pull_make_local_copy(i->temp_path, i->image_root, i->local, i->force_local);
        if (r < 0)
                return r;

        return 0;
}

static int aci_pull_job_on_open_disk(PullJob *j) {
        AciPull *i;
        int r;

        assert(j);
        assert(j->userdata);

        i = j->userdata;

        if (!i->final_path) {
                i->final_path = strjoin(i->image_root, "/.aci-", i->id, NULL);
                if (!i->final_path)
                        return log_oom();
        }
        assert(i->final_path);

        if (!i->temp_path) {
                r = tempfn_random(i->final_path, &i->temp_path);
                if (r < 0)
                        return log_oom();

                mkdir_parents_label(i->temp_path, 0700);

                r = btrfs_subvol_make(i->temp_path);
                if (r < 0)
                        return log_error_errno(r, "Failed to make btrfs subvolume %s: %m", i->temp_path);
        }

        assert(i->tar_pid <= 0);
        j->disk_fd = import_fork_tar_x(i->temp_path, &i->tar_pid);
        if (j->disk_fd < 0)
                return j->disk_fd;

        return 0;
}

static void aci_pull_job_on_progress(PullJob *j) {
        AciPull *i;

        assert(j);
        assert(j->userdata);

        i = j->userdata;

        aci_pull_report_progress(
                        i,
                        j == i->simple_discovery_job             ? ACI_SIMPLE_DISCOVERY :
                        j == i->meta_discovery_job               ? ACI_META_DISCOVERY :
                                                                   ACI_DOWNLOADING);
}
static int aci_pull_start_meta_discovery(AciPull *i) {
        int r;

        r = pull_job_new(&i->meta_discovery_job, "https://coreos.com", i->glue, i);
        if (r < 0)
                return log_error_errno(r, "Failed to allocate meta discovery job: %m");

        i->meta_discovery_job->on_finished = aci_pull_job_on_finished;
        i->meta_discovery_job->on_progress = aci_pull_job_on_progress;
        i->meta_discovery_job->grow_machine_directory = i->grow_machine_directory;

        r = pull_job_begin(i->meta_discovery_job);
        if (r < 0)
                return log_error_errno(r, "Failed to start metadata discovery: %m");

        return 0;

}

static int aci_pull_start_download(AciPull *i, const char *url) {
        int r;

        r = pull_job_new(&i->download_job, url, i->glue, i);
        if (r < 0)
                return log_error_errno(r, "Failed to allocate meta discovery job: %m");

        i->download_job->on_finished = aci_pull_job_on_finished;
        i->download_job->on_open_disk = aci_pull_job_on_open_disk;
        i->download_job->on_progress = aci_pull_job_on_progress;
        i->download_job->grow_machine_directory = i->grow_machine_directory;

        r = pull_job_begin(i->download_job);
        if (r < 0)
                return log_error_errno(r, "Failed to start download: %m");

        return 0;

}

static void aci_pull_job_on_finished(PullJob *j) {
        AciPull *i;
        int r;

        assert(j);
        assert(j->userdata);

        i = j->userdata;

        if (i->simple_discovery_job == j) {
                if (j->error < 0) {
                        if (i->tar_pid > 1) {
                                (void) kill_and_sigcont(i->tar_pid, SIGKILL);
                                (void) wait_for_terminate(i->tar_pid, NULL);
                                i->tar_pid = 0;
                        }

                        r = aci_pull_start_meta_discovery(i);
                        if (r < 0)
                                goto finish;
                        return;
                } else
                        goto copy;

        } else if (i->meta_discovery_job == j) {
                if (j->error < 0) {
                        log_error_errno(j->error, "Failed to perform meta discovery");
                        r = j->error;
                        goto finish;
                }
                r = aci_pull_start_download(i, "https://github.com/coreos/etcd/releases/download/v2.0.5/etcd-v2.0.5-linux-amd64.aci");
                if (r < 0)
                        goto finish;
                return;

        } else if (i->download_job == j) {
                if (j->error < 0) {
                        log_error_errno(j->error, "Failed to download");
                        r = j->error;
                        goto finish;
                 }
                goto copy;

        } else
                assert_not_reached("Got finished event for unknown curl object");

copy:
        aci_pull_report_progress(i, ACI_COPYING);

        j->disk_fd = safe_close(j->disk_fd);
        if (i->tar_pid > 0) {
                r = wait_for_terminate_and_warn("tar", i->tar_pid, true);
                i->tar_pid = 0;
                if (r < 0)
                        goto finish;
        }

        r = aci_pull_make_local_copy(i);
        if (r < 0)
                goto finish;

        r = 0;

finish:
        if (i->on_finished)
                i->on_finished(i, r, i->userdata);
        else
                sd_event_exit(i->event, r);
}

int aci_pull_start(AciPull *i, const char *name, const char *tag, const char *local, bool force_local) {
        struct utsname u;
        const char *url;
        char *version, *os, *arch, *ext;
        int r;

        assert(i);

        if (!aci_name_is_valid(name))
                return -EINVAL;

        if (local && !machine_name_is_valid(local))
                return -EINVAL;

        if (i->simple_discovery_job)
                return -EBUSY;

        /* Get version, os, arch, ext attributes */
        version = strdupa(tag); /* "v2.0.0" or "verion=v2.0.0,foo=bar" */
        assert_se(uname(&u) >= 0);
        os = strdupa(u.sysname);
        os[0] = tolower(os[0]);
        arch = strdupa(architecture_to_string(uname_architecture()));
        ext = strdupa("aci");

        /* FIXME */
        i->id = malloc(4096);
        strcpy(i->id, "68b329da9893e34099c7d8ad5cb9c940");

        r = free_and_strdup(&i->local, local);
        if (r < 0)
                return r;
        i->force_local = force_local;

        r = free_and_strdup(&i->name, name);
        if (r < 0)
                return r;

        /* FIXME/HACK: systemd uses "x86-64", Rocket uses "amd64" ?? */
        if (streq(arch, "x86-64"))
                arch = strdupa("amd64");

        url = strjoina(PROTOCOL_PREFIX, name, "-", version, "-", os, "-", arch, ".", ext);

        r = pull_job_new(&i->simple_discovery_job, url, i->glue, i);
        if (r < 0)
                return r;

        i->simple_discovery_job->on_finished = aci_pull_job_on_finished;
        i->simple_discovery_job->on_open_disk = aci_pull_job_on_open_disk;
        i->simple_discovery_job->on_progress = aci_pull_job_on_progress;

        return pull_job_begin(i->simple_discovery_job);
}
