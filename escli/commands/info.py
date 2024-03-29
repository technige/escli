#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2021 Nigel Small
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from escli.commands import Command


class InfoCommand(Command):
    """ Display backend system information.
    """

    def get_name(self):
        return "info"

    def get_description(self):
        return self.__doc__.strip()

    def register(self, subparsers):
        parser = subparsers.add_parser(self.get_name(), description=self.get_description())
        # TODO: options to control format of output
        parser.set_defaults(f=self.info)
        return parser

    def info(self, args):
        """ Retrieve and display system information.
        """
        from json import dumps
        print(dumps(self.spi.client.info(), indent=2))
