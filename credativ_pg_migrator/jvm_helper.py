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

Since the process is terminating anyway there is nothing worth waiting for, so
we tell JPype to skip that wait, and we detach worker threads from the JVM when
they close their connection.
"""

import threading


def configure_jvm_shutdown():
    """
    Prevent JPype from blocking the interpreter shutdown.

    Safe to call repeatedly and also when the JDBC stack is not used at all -
    the setting is only evaluated by JPype's atexit handler.
    """
    try:
        import jpype.config
        jpype.config.destroy_jvm = False
    except Exception:
        # jpype is not installed or too old - nothing to configure
        pass


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
