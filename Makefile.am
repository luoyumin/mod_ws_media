# In-tree build (classic FreeSWITCH module layout).
#
# Use this when you drop mod_ws_media into a FreeSWITCH *source* tree, exactly
# like the bundled modules (mod_unimrcp, etc.):
#
#   1. Copy/symlink this directory to  freeswitch/src/mod/applications/mod_ws_media
#   2. Add  applications/mod_ws_media  to  freeswitch/modules.conf
#   3. Rebuild FreeSWITCH (or:  cd src/mod/applications/mod_ws_media && make )
#
# For a standalone / out-of-tree build against an installed FreeSWITCH,
# use the plain `Makefile` in this repo instead (make && sudo make install).

include $(top_srcdir)/build/modmake.rulesam
MODNAME=mod_ws_media

mod_LTLIBRARIES = mod_ws_media.la
mod_ws_media_la_SOURCES  = mod_ws_media.c
mod_ws_media_la_CFLAGS   = $(AM_CFLAGS)
mod_ws_media_la_LIBADD   = $(switch_builddir)/libfreeswitch.la -lssl -lcrypto
mod_ws_media_la_LDFLAGS  = -avoid-version -module -no-undefined -shared
