/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.priam.cli;

import com.netflix.priam.backup.parallel.IncrementalBackupProducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IncrementalBackuper {
    private static final Logger logger = LoggerFactory.getLogger(IncrementalBackuper.class);

    public static void main(String[] args) {
        try {
            Application.initialize();
            final IncrementalBackupProducer backup = Application.getInjector().getInstance(IncrementalBackupProducer.class);
            try {
                backup.executeSync();
            } catch (Exception e) {
                logger.error("Unable to complete incremental backup: ", e);
            }
        } finally {
            Application.shutdownAdditionalThreads();
        }
    }
}