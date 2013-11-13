/**
 * Copyright 2013 Andrew Yates <andrew@ir.cs.georgetown.edu>
 *
 * Based on hbc-example, which is  
 * Copyright 2013 Twitter, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package edu.georgetown.cs.ir.hbc;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.GZIPOutputStream;

public class Sample {
    String outDir = null;

    public Sample(String outDir) {
        this.outDir = outDir;

        File f = new File(this.outDir);
        f.mkdir();
    }

    public void oauth(String consumerKey, String consumerSecret, String token, String secret) throws InterruptedException {
        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(100000);
        StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
        endpoint.stallWarnings(true);

        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);

        // Create a new BasicClient. By default gzip is enabled.
        BasicClient client = new ClientBuilder()
            .hosts(Constants.STREAM_HOST)
            .endpoint(endpoint)
            .authentication(auth)
            .processor(new StringDelimitedProcessor(queue))
            .build();

        Thread t = spawnOutputThread(outDir, queue);

        client.connect();

        while (t.isAlive()) {
            t.join(1000);
            if (client.isDone()) {
                System.out.println("Connection closed: " + client.getExitEvent().getMessage());
                break;
            }
        }

        client.stop();
    }

    public Thread spawnOutputThread(final String outDir, final BlockingQueue<String> queue) {
        Thread t = new Thread() {
                public void run() {
                    try {
                        // create a new compressed file every 400k messages
                        while (true) {
                            long time = System.currentTimeMillis() / 1000L;
                            String fn = outDir + "/" + time + ".gz";

                            Writer writer = new OutputStreamWriter(new GZIPOutputStream(
                                                                       new FileOutputStream(fn)), "UTF-8");

                            for (int i=0; i<400000; i++) {
                                String msg = queue.take();
                                writer.write(msg);
                            }

                            writer.close();
                        }
                    } catch (InterruptedException ie) {
                        System.err.println("Thread interrupted:" + ie);
                    } catch(Exception e) {
                        System.err.println("Thread exception:" + e);
                        System.exit(3);
                    }
                }  
            };

        t.start();

        return t;
    }

    public static void main(String[] args) {
        if (args.length != 5) {
            System.err.println("usage: <consumer key> <consumer secret> <token> <secret> <output directory>");
            System.exit(1);
        }

        Sample sample = new Sample(args[4]);

        try {
            sample.oauth(args[0], args[1], args[2], args[3]);
        } catch (InterruptedException e) {
            System.err.println(e);
            System.exit(2);
        }
    }
}
