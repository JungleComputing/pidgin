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

        final PidginImpl pidgin;
        int count;

        PidginData(PidginImpl pidgin) {
            this.pidgin = pidgin;
            this.count = 0;
        }

        void inc() {
            count = count + 1;
        }

        void dec() {
            count = count - 1;
        }
    }

    private static HashMap<String, PidginData> flock = new HashMap<>();

    public static synchronized Pidgin create(String name, Properties p) throws Exception {

        PidginData tmp = flock.get(name);

        if (tmp == null) {
            tmp = new PidginData(new PidginImpl(p));
            flock.put(name, tmp);
        }

        tmp.inc();
        return tmp.pidgin;
    }

    public static synchronized Pidgin get(String name) {

        PidginData tmp = flock.get(name);

        if (tmp == null) {
            return null;
        }

        tmp.inc();
        return tmp.pidgin;
    }

    public static synchronized void terminate(String name) throws IOException {

        PidginData tmp = flock.get(name);

        if (tmp == null) {
            return;
        }

        tmp.dec();

        if (tmp.count == 0) {
            tmp.pidgin.terminate();
            flock.remove(name);
        }
    }
}
