/**
 * Copyright 2017 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.backup.parallel;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.*;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.notification.BackupNotificationMgr;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.TaskTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

@Singleton
public class IncrementalBackupProducer extends AbstractBackup {

    public static final String JOBNAME = "ParallelIncremental";
    private static final Logger logger = LoggerFactory.getLogger(IncrementalBackupProducer.class);

    private final List<String> incrementalRemotePaths = new ArrayList<String>();
    private IncrementalConsumerMgr incrementalConsumerMgr;
    private ITaskQueueMgr<AbstractBackupPath> taskQueueMgr;
    private BackupRestoreUtil backupRestoreUtil;

    @Inject
    public IncrementalBackupProducer(IConfiguration config, Provider<AbstractBackupPath> pathFactory, IFileSystemContext backupFileSystemCtx
            , @Named("backup") ITaskQueueMgr taskQueueMgr
            , BackupNotificationMgr backupNotificationMgr
    ) {

        super(config, backupFileSystemCtx, pathFactory, backupNotificationMgr);
        this.taskQueueMgr = taskQueueMgr;

        init(backupFileSystemCtx);
    }

    private void init(IFileSystemContext backupFileSystemCtx) {
        backupRestoreUtil = new BackupRestoreUtil(config.getIncrementalKeyspaceFilters(), config.getIncrementalCFFilter());
        //"this" is a producer, lets wake up the "consumers"
        this.incrementalConsumerMgr = new IncrementalConsumerMgr(this.taskQueueMgr, backupFileSystemCtx.getFileStrategy(config), super.config);
        Thread consumerMgr = new Thread(this.incrementalConsumerMgr);
        consumerMgr.start();
    }

    @Override
    protected void backupUploadFlow(File backupDir) {
        for (final File file : backupDir.listFiles()) {
            try {
                final AbstractBackupPath bp = pathFactory.get();
                bp.parseLocal(file, BackupFileType.SST);
                this.taskQueueMgr.add(bp); //producer -- populate the queue of files.  *Note: producer will block if queue is full.
            } catch (Exception e) {
                logger.warn("Unable to queue incremental file, treating as non-fatal and moving on to next.  Msg: {} Fail to queue file: {}",
                        e.getLocalizedMessage(), file.getAbsolutePath());
            }
        // end enqueuing all incremental files for a CF
        }
    }

    @Override
    /*
     * Keeping track of successfully uploaded files.
	 */
    protected void addToRemotePath(String remotePath) {
        incrementalRemotePaths.add(remotePath);
    }


    @Override
    public void execute() throws Exception {
        // Clearing remotePath List
        incrementalRemotePaths.clear();
        initiateBackup("backups", backupRestoreUtil);
        return;
    }

    // For backwards compatibility with the old Sync method of incrementals
    public void executeSync() throws Exception {
        execute();
        CountDownLatch backupDone = new CountDownLatch(1);
        new Thread(() -> {
            while (!taskQueueMgr.allTasksCompleted()) {
                try {
                    logger.info("Still not done: {}", taskQueueMgr.allTasksCompleted());
                    logger.info("wat: {}", taskQueueMgr.getNumOfTasksToBeProcessed());
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    logger.error("Got interrupted before incremental backup finished");
                }
            }
            backupDone.countDown();
        }).start();
        // Wait for incremental backup to finish
        backupDone.await();
    }

	/**
	 * @return an identifier of purpose of the task.
	 */
    @Override
    public String getName() {
        return JOBNAME;
    }

    public static TaskTimer getTimer(IConfiguration config) {
        return new SimpleTimer(JOBNAME, config.getIncrementalBkupIntervalMs());
    }
}