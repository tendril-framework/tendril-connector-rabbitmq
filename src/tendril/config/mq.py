# Copyright (C) 2019 Chintalagiri Shashank
#
# This file is part of Tendril.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""
Paths Configuration Options
===========================
"""

import pika

from tendril.utils.config import ConfigOption
from tendril.utils.config import ConfigOptionConstruct
from tendril.utils import log
logger = log.get_logger(__name__, log.DEFAULT)

depends = ['tendril.config.core',
           'tendril.config.mq_core']


class RabbitMQConnectionParameters(ConfigOptionConstruct):
    @property
    def value(self):
        return pika.ConnectionParameters(
            host=self.ctx["MQ{}_SERVER_HOST".format(self._parameters)],
            port=self.ctx["MQ{}_SERVER_PORT".format(self._parameters)],
            virtual_host=self.ctx["MQ{}_SERVER_VIRTUALHOST".format(self._parameters)],
            credentials=pika.PlainCredentials(
                self.ctx["MQ{}_SERVER_USERNAME".format(self._parameters)],
                self.ctx["MQ{}_SERVER_PASSWORD".format(self._parameters)])
        )


def _rabbitmq_config_template(mq_code):
    return [
        ConfigOption(
            'MQ{}_ENABLED'.format(mq_code),
            'False',
            'Whether to enable the {} MQ Server. This only controls whether tendril '
            'will proactively attempt to establish an MQ Server connection. If code '
            'otherwise results in an attemt to make the connection, this will not '
            'stop it.'.format(mq_code)
        ),
        ConfigOption(
            'MQ{}_PROVIDER'.format(mq_code),
            '"amqp"',
            'Provider type for the {} MQ Server'.format(mq_code)
        ),
        ConfigOption(
            'MQ{}_SERVER_HOST'.format(mq_code),
            "'localhost'",
            "Server Host for the {} MQ Server".format(mq_code)
        ),
        ConfigOption(
            'MQ{}_SERVER_PORT'.format(mq_code),
            "5672",
            "Server Port for the {} MQ Server".format(mq_code)
        ),
        ConfigOption(
            'MQ{}_SERVER_VIRTUALHOST'.format(mq_code),
            "'tendril'",
            "VirtualHost to use for the {} MQ Server. "
            "All MQ Connections from tendril will use this virtual "
            "host unless locally overridden in some as yet "
            "unspecified way.".format(mq_code)
        ),
        ConfigOption(
            'MQ{}_SERVER_USERNAME'.format(mq_code),
            "'tendril'",
            "Username to use for the {} MQ Server.".format(mq_code)
        ),
        ConfigOption(
            'MQ{}_SERVER_PASSWORD'.format(mq_code),
            "'tendril'",
            "Server Password to use for the {} MQ Server.".format(mq_code),
            masked=True
        ),
        ConfigOption(
            'MQ{}_SERVER_SSL'.format(mq_code),
            "False",
            "Whether to use SSL when connecting to "
            "the {} MQ Server.".format(mq_code)
        ),
        ConfigOption(
            'MQ{}_SERVER_EXCHANGE'.format(mq_code),
            "'tendril.topic'",
            "RabbitMQ Server Exchange to use for the {} MQ Server. "
            "All MQ Connections from tendril will use this exchange "
            "unless locally overridden in some as yet unspecified way.".format(mq_code)
        ),
        RabbitMQConnectionParameters(
            'MQ{}_SERVER_PARAMETERS'.format(mq_code),
            mq_code,
            "Constructed PikaConnection parameters instance. This option "
            "is created by the code, and should not be set directly in any "
            "config file."
        )
    ]


def load(manager):
    logger.debug("Loading {0}".format(__name__))
    config_elements_mq = []
    for code in manager.MQ_SERVER_CODES:
        config_elements_mq += _rabbitmq_config_template(code)
    manager.load_elements(config_elements_mq,
                          doc="Tendril RabbitMQ Configuration")
