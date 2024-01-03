/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class CalculateAverage_gerdaschemann {

    static final String FILE = "./measurements.txt";
    // private static int NO_OF_ENTRIES = 1000000000;

    static void debug(String fmt, Object... args) {
        System.err.println(String.format(fmt, args));
    }

    record Measurement(double min, double max, double sum, long count) {

        Measurement(double initialMeasurement) {
            this(initialMeasurement, initialMeasurement, initialMeasurement, 1);
        }

        static Measurement combineWith(Measurement m1, Measurement m2) {
            return new Measurement(
                    Math.min(m1.min, m2.min),
                    Math.max(m1.max, m2.max),
                    m1.sum + m2.sum,
                    m1.count + m2.count
            );
        }

        public String toString() {
            return round(min) + "/" + round(sum / count) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    static class Result {
        Map<String, Measurement> result = new HashMap<>();

        Result() {
        }

        // Need this for debugging
        // private Integer blockNo;
        // Result(final int blockNo) {
        // this.blockNo = blockNo;
        // }

        void merge(final String key, final Measurement toMerge) {
            Measurement value = result.get(key);
            if (null != value) {
                result.put(key, Measurement.combineWith(value, toMerge));
            }
            else {
                result.put(key, toMerge);
            }
        }

        void merge(final Result toMerge) {
            // debug("Merging in %d results from block #%d",
            // toMerge.result.keySet().size(), toMerge.blockNo);
            for (String key : toMerge.result.keySet()) {
                merge(key, toMerge.result.get(key));
            }
        }
    }

    static class BlockAlgorithm {

        static class Block {

            byte[] data;
            String preData;

            int blockNo;
            long lastIndex;

            Block(final int blockNo, final int blockSize) {
                data = new byte[blockSize];
                lastIndex = blockSize - 1;
                this.blockNo = blockNo;
            }

            void prepend(final Block previousBlock) {
                int lastIndex = (int) previousBlock.lastIndex;
                int index = lastIndex - 1;
                while (previousBlock.data[index] != '\n') {
                    index--;
                }
                index++;
                preData = new String(previousBlock.data, index, lastIndex - index);
                // debug("Prepended data '%s' (%d bytes) for block #%d",
                // preData, lastIndex - index, blockNo);
                previousBlock.lastIndex = index;
            }

            int index = 0;

            void handleFirst(Result result) {
                while (data[index] != '\n') {
                    index++;
                }
                String line = (null == preData)
                        ? new String(data, 0, index)
                        : preData + new String(data, 0, index);
                int semikolonIndex = line.indexOf(';');
                String key = line.substring(0, semikolonIndex);
                String value = line.substring(semikolonIndex + 1);
                result.merge(key, new Measurement(Double.parseDouble(value)));
                index++;
            }

            Result count() {
                // debug("Counting in block %d / %d", blockNo, blocks.size());
                Result result = new Result();
                handleFirst(result);
                // int newLines = 1;

                int startOfKey = index;
                int lenOfKey = 0;
                int startOfValue = 0;
                int lenOfValue;

                while (index < lastIndex) {
                    if (data[index] == ';') {
                        lenOfKey = index - startOfKey;
                        startOfValue = index + 1;
                    }
                    else if (data[index] == '\n') {
                        lenOfValue = index - startOfValue;
                        String key = new String(data, startOfKey, lenOfKey);
                        String value = new String(data, startOfValue, lenOfValue);
                        result.merge(key,
                                new Measurement(Double.parseDouble(value)));
                        startOfKey = index + 1;
                        // newLines++;
                    }
                    else if (data[index] == '\0') {
                        throw new RuntimeException("This should never happen: Data should not contain 0 bytes");
                    }
                    index++;
                }
                // debug("Block #%d has %d newLines", blockNo, newLines);
                return result;
            }
        }

        // private static int BLOCKSIZE = Integer.MAX_VALUE - 5;
        List<Block> blocks;
        final int noOfThreads = Runtime.getRuntime().availableProcessors();
        Result result = new Result();

        BlockAlgorithm() {
            // debug("System has %d cores", noOfThreads);
        }

        void readBlocks() throws Exception {
            // Should we parallelize I/O as well? Currently, it makes up only 1.x seconds on my machine ...
            try (RandomAccessFile file = new RandomAccessFile(FILE, "r")) {
                long fileLength = file.length();
                int blockSize = (int) (fileLength / noOfThreads) + 1;
                blocks = new ArrayList<>(noOfThreads);
                // debug("File '%s' has length = %d bytes (= %d blocks of size %d)",
                // FILE, fileLength, noOfThreads, blockSize);
                Block previousBlock = null;
                int blockNo = 0;
                for (long startPosition = 0; startPosition < fileLength; startPosition += blockSize) {
                    file.seek(startPosition);
                    Block block = new Block(blockNo++, blockSize);
                    block.lastIndex = file.read(block.data);
                    blocks.add(block);
                    if (null != previousBlock && previousBlock.data[blockSize - 1] != '\n') {
                        block.prepend(previousBlock);
                    }
                    previousBlock = block;
                    // debug("Start of Block (#%d): '%s'",
                    // block.blockNo, new String(block.data, 0, 5));
                    // debug("End of Block (#%d): '%s'",
                    // block.blockNo, new String(block.data, (int) readBytes - 5, 5));
                    // debug("Read another %010d bytes (%010d total) / %010d bytes", readBytes, startPosition + readBytes, fileLength);
                }
                assert (noOfThreads == blocks.size());
            }
        }

        void count() throws Exception {
            try (ExecutorService executorService = Executors.newFixedThreadPool(noOfThreads)) {
                List<Callable<Result>> taskList = new ArrayList<>();
                for (int blockNo = 0; blockNo < noOfThreads; blockNo++) {
                    final int finalBlockNo = blockNo;
                    taskList.add(() -> blocks.get(finalBlockNo).count());
                }

                List<Future<Result>> resultList = executorService.invokeAll(taskList);
                // int total = 0;
                for (Future<Result> future : resultList) {
                    // total += future.get(); // will wait if not ready yet
                    result.merge(future.get());
                }
                // debug("Found #%d newLines / semicolons", total);
                executorService.shutdown();
            }
        }

        void print(PrintStream out) {
            out.print("{");
            out.print(result.result.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Object::toString).collect(Collectors.joining(", ")));
            out.println("}");
        }
    }

    public static void main(String[] args) throws Exception {
        BlockAlgorithm ba = new BlockAlgorithm();
        long before = System.currentTimeMillis();
        ba.readBlocks();
        debug("Reading data took: %d ms", System.currentTimeMillis() - before);
        before = System.currentTimeMillis();
        ba.count();
        debug("Counting took: %d ms", System.currentTimeMillis() - before);
        before = System.currentTimeMillis();
        ba.print(System.out);
        debug("Printing took: %d ms", System.currentTimeMillis() - before);
    }
}
