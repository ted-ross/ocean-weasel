#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

from proton  import Messenger, Message
from pymongo import MongoClient
import sys

DATABASE  = "amq"
AMQP_HOST = "0.0.0.0:5672"
SERVICE   = "amqp:/mongo.amq"

class Adapter(object):
    def __init__(self):
        self.db  = MongoClient()[DATABASE]
        self.mng = Messenger()
        self.mng.route("amqp:/*", "amqp://%s/$1" % AMQP_HOST)
        self.mng.start()
        self.mng.subscribe(SERVICE)

    def run(self):
        while (True):
            if self.mng.incoming < 1:
                self.mng.recv(1)
            if self.mng.incoming > 0:
                request = Message()
                self.mng.get(request)
                if request.reply_to:
                    response = Message()
                    response.address = request.reply_to
                    response.correlation_id = request.correlation_id
                    response.properties = {}
                    response.properties['result'], response.body = self.process(request.properties, request.body)
                    self.mng.put(response)
                    self.mng.send()

    def process(self, properties, body):
        if 'collection' not in properties:
            return 'Error: collection not specified', None
        if 'command' not in properties:
            return 'Error: command not specified', None
        collection = properties['collection']
        command    = properties['command']
        col = self.db[collection]
        if command == 'find':
            if body == None:
                cursor = col.find()
            elif body.__class__ == dict:
                cursor = col.find(body)
            elif body.__class__ == list:
                cursor = col.find(body[0], body[1])
            answer = []
            for i in range(cursor.count()):
                answer.append(cursor[i])
        return 'OK', answer


def main(argv=None):
    adapter = Adapter()
    adapter.run()
    return 0

if __name__ == "__main__":
    sys.exit(main())
