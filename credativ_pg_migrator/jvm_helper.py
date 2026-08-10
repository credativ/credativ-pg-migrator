# credativ-pg-migrator
# Copyright (C) 2025 credativ GmbH
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
Helpers for the JVM used by the JDBC based connectors (jaydebeapi / JPype).

JPype registers an atexit handler which calls DestroyJavaVM. Despite its name
DestroyJavaVM does not destroy anything - it waits until all non daemon Java
threads have finished. JDBC drivers (Informix in particular) keep such threads
running, and every Python worker thread which opened a JDBC connection stays
registered in the JVM as a non daemon thread as well, because jaydebeapi
attaches threads to the JVM but never detaches them again. The result is that
the migrator prints its final summary and then hangs forever in the interpreter
shutdown.

Skipping only the wait (jpype.config.destroy_jvm = False) is not an option - the
JVM then aborts with "FATAL: exception not rethrown" in roughly half of all runs,
because JPype tears its state down while driver threads are still alive. The
migrator has finished all of its work at that point, so the process is ended
directly instead and the operating system reclaims everything.
"""

import logging
import os
import sys
import threading


def is_jvm_running():
    """ True when a JDBC connector has started a JVM in this process """
    try:
        import jpype
        return jpype.isJVMStarted()
    except Exception:
        # jpype is not installed - no JDBC connector was used
        return False


def terminate_process(status=0):
    """
    End the migrator with the given exit status.

    Without a running JVM this is an ordinary exit. With one the process is ended
    immediately, because both shutdown paths offered by JPype are unusable once a
    JDBC driver has been loaded - see the module docstring. All output is flushed
    and the logging handlers are closed first, since that is what the skipped
    atexit handlers would have done.
    """
    logging.shutdown()
    sys.stdout.flush()
    sys.stderr.flush()
    if is_jvm_running():
        os._exit(status)
    sys.exit(status)


def detach_thread_from_jvm():
    """
    Detach the current thread from the JVM if it was attached by jaydebeapi.

    Called after a JDBC connection is closed so that finished worker threads do
    not leak JVM thread structures. The main thread is left attached because it
    keeps running migrator code which may still touch Java objects. JPype
    re-attaches a thread automatically when it uses Java again, so detaching
    does not break a later reconnect.
    """
    if threading.current_thread() is threading.main_thread():
        return
    try:
        import jpype
        if jpype.isJVMStarted() and jpype.isThreadAttachedToJVM():
            jpype.detachThreadFromJVM()
    except Exception:
        # detaching is a best effort cleanup, never let it break the migration
        pass
