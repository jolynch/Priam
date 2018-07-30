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

import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.utils.RetryableCallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Performs an upload of a file, with retries.
 */
public class IncrementalConsumer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(IncrementalConsumer.class);

    private AbstractBackupPath bp;
    private IBackupFileSystem fs;
    private BackupPostProcessingCallback<AbstractBackupPath> callback;

    /**
     * Upload files. Does not delete the file in case of
     * error.
     *
     * @param bp - the file to upload with additional metada
     */
    public IncrementalConsumer(AbstractBackupPath bp, IBackupFileSystem fs
            , BackupPostProcessingCallback<AbstractBackupPath> callback
    ) {
        this.bp = bp;
        this.bp.setType(AbstractBackupPath.BackupFileType.SST);  //Tag this is an incremental upload, not snapshot
        this.fs = fs;
        this.callback = callback;
    }

    @Override
    /*
     * Upload specified file, with retries logic.
	 * File will be deleted only if uploaded successfully.
	 */
    public void run() {

        logger.info("Consumer - about to upload file: {}", this.bp.getFileName());

        try {
            // Allow up to 30s of arbitrary failures at the top level. The upload call itself typically has retries
            // as well so this top level retry is on top of those retries. Assuming that each call to upload has
            // ~30s maximum of retries this yields about 3.5 minutes of retries at the top level since
            // (6 * (5 + 30) = 210 seconds). Even if this fails, however, the upload will be re-enqueued
            new RetryableCallable<Void>(6, 5000) {
                @Override
                public Void retriableCall() throws Exception {

                    java.io.InputStream is = null;
                    try {
                        is = bp.localReader();
                    } catch (java.io.FileNotFoundException | RuntimeException e) {
                        if (is != null) {
                            is.close();
                        }
                        throw new java.util.concurrent.CancellationException("Someone beat me to uploading this file"
                                + ", no need to retry.  Most likely not needed but to be safe, checked and released handle to file if appropriate.");
                    }

                    try {
                        if (is == null) {
                            throw new NullPointerException("Unable to get handle on file: " + bp.getFileName());
                        }
                        // Important context: this upload call typically has internal retries but those are only
                        // to cover over very temporary (<10s) network partitions. For larger partitions re rely on
                        // higher up retries and re-enqueues.
                        fs.upload(bp, is);
                        bp.setCompressedFileSize(fs.getBytesUploaded());
                        bp.setAWSSlowDownExceptionCounter(fs.getAWSSlowDownExceptionCounter());
                        return null;
                    } catch (Exception e) {
                        logger.error("Exception uploading local file {},  releasing handle, and will retry.", bp.getFileName());
                        if (is != null) {
                            is.close();
                        }
                        throw e;
                    }
                }
            }.call();
            // Clean up the underlying file.
            bp.getBackupFile().delete();
        } catch (Exception e) {
            if (e instanceof java.util.concurrent.CancellationException) {
                logger.debug("Failed to upload local file {}. Ignoring to continue with rest of backup.  Msg: {}", this.bp.getFileName(), e.getLocalizedMessage());
            } else {
                logger.error("Failed to upload local file {}. Ignoring to continue with rest of backup.  Msg: {}", this.bp.getFileName(), e.getLocalizedMessage());
            }
        } finally {
            // post processing must happen regardless of the outcome of the upload. Otherwise we can
            // leak tasks into the underlying taskMgr queue and prevent re-enqueues of the upload itself
            this.callback.postProcessing(bp);
        }
        logger.info("Consumer - done with upload file: {}", this.bp.getFileName());
    }

}