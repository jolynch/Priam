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

/*
 * 
 * Represents a queue of tasks to be completed.
 * 
 * Duties of the mgr include:
 * - Mechanism to add a task, including deduplication of tasks before adding to queue.
 * - Guarantee delivery of task to only one consumer.
 * - Provide relevant metrics including number of tasks in queue, number of tasks processed.
 */
public interface ITaskQueueMgr<E> {

    /**
     * Adds the provided task into the queue if it does not already exist. For performance reasons
     * this is best effort and therefore callers are responsible for handling duplicate tasks.
     *
     * This method will block if the queue of tasks is full
     * @param task The task to put onto the queue
     */
    void add(E task);

    /**
     * Adds the provided task into the queue if it does not already exist. For performance reasons
     * this is best effort and therefore callers are responsible for handling duplicate tasks.
     *
     * This method should not block. If no implementation is provided however this is equivalent to
     * {@link #add(Object)}.
     *
     * @param task The task to put onto the queue
     * @return if the task was successfully added to the queue. True means yes, False means no.
     */
    default boolean offer(E task) {
        add(task);
        return true;
    }

    /**
     * @return task, null if none is available.
     */
    E take() throws InterruptedException;

    /**
     * @return true if there are tasks within queue to be processed; false otherwise.
     */
    Boolean hasTasks();

    /**
     * A means to perform any post processing once the task has been completed.  If post processing is needed,
     * the consumer should notify this behavior via callback once the task is completed.
     *
     * *Note: "completed" here can mean success or failure.
     */
    void taskPostProcessing(E completedTask);


    Integer getNumOfTasksToBeProcessed();

    /**
     * @return true if all tasks completed (includes failures) for a date; false, if at least 1 task is still in queue.
     */
    Boolean tasksCompleted(java.util.Date date);
}