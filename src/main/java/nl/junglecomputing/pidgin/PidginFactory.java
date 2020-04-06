/*
 * Copyright 2020 Netherlands eScience Center
 *                Vrije Universiteit Amsterdam
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.junglecomputing.pidgin;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

import nl.junglecomputing.pidgin.impl.ibis.PidginImpl;

public class PidginFactory {

    private static class PidginData {

        PidginImpl pidgin;
        int count;

        PidginData() {
            this.count = 0;
        }

        synchronized void setPidgin(PidginImpl p) {
            this.pidgin = p;
        }

        synchronized PidginImpl getPidgin() {
            return pidgin;
        }

        synchronized int inc() {
            return count++;
        }

        synchronized int dec() {
            return --count;
        }
    }

    private static HashMap<String, PidginData> flock = new HashMap<>();

    public static Pidgin create(String name, Properties p) throws Exception {

        PidginData tmp = null;

        synchronized (flock) {

            tmp = flock.get(name);

            if (tmp == null) {
                tmp = new PidginData();
                flock.put(name, tmp);
            }

            tmp.inc();
        }

        PidginImpl pidgin = tmp.getPidgin();

        if (pidgin == null) {
            pidgin = new PidginImpl(p);
            tmp.setPidgin(pidgin);
        }

        return pidgin;
    }

    public static void terminate(String name) throws IOException {

        PidginImpl toKill = null;

        synchronized (flock) {

            PidginData tmp = flock.get(name);

            if (tmp == null) {
                return;
            }

            int count = tmp.dec();

            System.out.println("Pidgin " + name + " terminate count " + count);

            if (count <= 0) {
                flock.remove(name);
                toKill = tmp.pidgin;

            }
        }

        if (toKill != null) {
            toKill.terminate();
        }
    }
}
