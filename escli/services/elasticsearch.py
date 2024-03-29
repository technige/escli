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


from logging import getLogger

from elasticsearch import Elasticsearch, ConnectionError, AuthenticationException, TransportError

from escli.services import Client, ClientConnectionError, ClientAuthError, ClientAPIError


log = getLogger(__name__)


class ElasticsearchClient(Client):
    """ Client for use with Elasticsearch.
    """

    def __init__(self):
        with ElasticsearchExceptionWrapper():
            self._client = Elasticsearch(**self.get_settings_from_env())

    def info(self):
        with ElasticsearchExceptionWrapper():
            return dict(self._client.info())

    def get_indexes(self, include_all=False):
        pattern = "*" if include_all else "*,-.*"
        return self._client.indices.get(index=pattern)

    def create_index(self, name):
        self._client.indices.create(index=name)

    def delete_index(self, name):
        self._client.indices.delete(index=name)

    def search(self, target, query, fields=None, sort=None, page_size=10, page_number=1):
        with ElasticsearchExceptionWrapper():
            if query is None:
                query = {"match_all": {}}
            else:
                field, _, value = query.partition("=")
                query = {"match": {field: value}}
            if sort:
                if sort.startswith("~"):
                    sort = {sort[1:]: "desc"}
                else:
                    sort = {sort: "asc"}
            else:
                sort = None
            res = self._client.search(index=target, query=query, _source_includes=fields or "*",
                                      sort=sort, from_=(page_size * (page_number - 1)), size=page_size)
        return [hit["_source"] for hit in res["hits"]["hits"]]

    def ingest(self, target, document):
        with ElasticsearchExceptionWrapper():
            res = self._client.index(index=target, document=document)
        return res  # TODO: something more intelligent


class ElasticsearchExceptionWrapper:
    """ Wrapper to catch and promote exceptions to the appropriate level
    of abstraction.
    """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_type:
            return
        try:
            raise exc_val
        except ConnectionError as ex:
            log.debug(ex.info)
            raise ClientConnectionError("Connection error: %s" % ex) from ex
        except AuthenticationException as ex:
            log.debug(ex.info)
            raise ClientAuthError("Auth error: %s" % ex) from ex
        except TransportError as ex:
            log.debug(ex.info)
            raise ClientAPIError("API error: %s" % ex) from ex
