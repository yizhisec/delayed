# -*- coding: utf-8 -*-

import logging

from delayed.logger import logger, set_handler, setup_logger


def test_logger():
    assert logger.level == logging.DEBUG
    assert len(logger.handlers) == 1
    assert isinstance(logger.handlers[0], logging.NullHandler)

    setup_logger()
    assert logger.level == logging.DEBUG
    assert len(logger.handlers) == 1
    handler = logger.handlers[0]
    assert isinstance(handler, logging.StreamHandler)

    setup_logger()
    assert len(logger.handlers) == 1
    assert logger.handlers[0] is handler

    handler = logging.NullHandler()
    set_handler(handler)
    assert len(logger.handlers) == 1
    assert logger.handlers[0] is handler
