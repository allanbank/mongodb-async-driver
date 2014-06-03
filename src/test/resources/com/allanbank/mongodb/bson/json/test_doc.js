/*
 * #%L
 * test_doc.js - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
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
 * #L%
 */
/*
 * Copyright 2013, Allanbank Consulting, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
{
    boolean_1 : true,
    boolean_2 : false,
    n : null,
    int : 1,
    double : 1.0,
    double_1 : 1.0e12,
    "double_2" : 1e-1,
    string : 'abc',
    'string2' : "def",
    symbol : ghi,
    array : [
        1, 1.0, 1.0e12, 1e-1, 'abc', "def", ghi, { 
            int : 1,
            double : 1.0,
            double_1 : 1.0e12,
            double_2 : 1e-1,
            string : 'abc',
            string2 : "def",
            symbol : ghi
        }
    ],
    doc : {
        int : 1,
        double : 1.0,
        double_1 : 1.0e12,
        double_2 : 1e-1,
        string : 'abc',
        string2 : "def",
        symbol : ghi,
        array : [
            1, 1.0, 1.0e12, 1e-1, 'abc', "def", ghi
        ]
    }
}
