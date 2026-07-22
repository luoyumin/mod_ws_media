# mod_ws_media — out-of-tree build
#
# Build against an installed FreeSWITCH (recommended, via pkg-config):
#     make
#     sudo make install
#
# Or point at a FreeSWITCH install prefix explicitly:
#     make FREESWITCH_PATH=/usr/local/freeswitch
#     sudo make install FREESWITCH_PATH=/usr/local/freeswitch
#
# On macOS with Homebrew OpenSSL you may need:
#     export PKG_CONFIG_PATH="$(brew --prefix openssl@3)/lib/pkgconfig:$PKG_CONFIG_PATH"
#
# See also Makefile.am for the classic in-tree build (drop into a
# FreeSWITCH source tree under src/mod/applications/).

MODNAME := mod_ws_media
CC      ?= cc

# ---------------------------------------------------------------------------
# FreeSWITCH discovery
#   Priority: FREESWITCH_PATH (install prefix) > pkg-config freeswitch
# ---------------------------------------------------------------------------
ifdef FREESWITCH_PATH
  FS_CFLAGS   := -I$(FREESWITCH_PATH)/include/freeswitch -I$(FREESWITCH_PATH)/include
  MODULES_DIR ?= $(FREESWITCH_PATH)/lib/freeswitch/mod
  CONF_DIR    ?= $(FREESWITCH_PATH)/etc/freeswitch/autoload_configs
else
  FS_CFLAGS   := $(shell pkg-config --cflags freeswitch 2>/dev/null)
  MODULES_DIR ?= $(shell pkg-config --variable=modulesdir freeswitch 2>/dev/null)
  CONF_DIR    ?= $(shell pkg-config --variable=confdir freeswitch 2>/dev/null)/autoload_configs
endif

# ---------------------------------------------------------------------------
# OpenSSL (required: TLS + WebSocket handshake SHA1/base64/RAND)
# ---------------------------------------------------------------------------
SSL_CFLAGS := $(shell pkg-config --cflags openssl 2>/dev/null)
SSL_LIBS   := $(shell pkg-config --libs openssl 2>/dev/null)
ifeq ($(strip $(SSL_LIBS)),)
  SSL_LIBS := -lssl -lcrypto
endif

CFLAGS  ?= -O2 -g
CFLAGS  += -fPIC -Wall -Wno-unused-parameter $(FS_CFLAGS) $(SSL_CFLAGS)
LDLIBS  += $(SSL_LIBS)

# ---------------------------------------------------------------------------
# Platform: FreeSWITCH loads modules via APR dso (dlopen); core switch_*
# symbols are resolved from the running FreeSWITCH process at load time.
# ---------------------------------------------------------------------------
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
  LDSHARED := -bundle -undefined dynamic_lookup
else
  LDSHARED := -shared
endif

TARGET := $(MODNAME).so

all: check-fs $(TARGET)

check-fs:
	@if [ -z "$(strip $(FS_CFLAGS))" ]; then \
	  echo "ERROR: FreeSWITCH development headers not found."; \
	  echo "  Either install the freeswitch dev headers so that"; \
	  echo "    pkg-config --exists freeswitch"; \
	  echo "  succeeds, or pass the install prefix explicitly:"; \
	  echo "    make FREESWITCH_PATH=/usr/local/freeswitch"; \
	  exit 1; \
	fi

$(TARGET): $(MODNAME).c
	$(CC) $(CFLAGS) $(LDSHARED) -o $@ $< $(LDLIBS)

install: $(TARGET)
	@test -n "$(strip $(MODULES_DIR))" || { \
	  echo "ERROR: module install dir unknown. Set FREESWITCH_PATH or MODULES_DIR."; exit 1; }
	install -d "$(DESTDIR)$(MODULES_DIR)"
	install -m 0755 $(TARGET) "$(DESTDIR)$(MODULES_DIR)/"
	@echo "installed: $(DESTDIR)$(MODULES_DIR)/$(TARGET)"
	@if [ -n "$(strip $(CONF_DIR))" ]; then \
	  if [ ! -f "$(DESTDIR)$(CONF_DIR)/ws_media.conf.xml" ]; then \
	    install -d "$(DESTDIR)$(CONF_DIR)"; \
	    install -m 0644 conf/autoload_configs/ws_media.conf.xml "$(DESTDIR)$(CONF_DIR)/"; \
	    echo "installed: $(DESTDIR)$(CONF_DIR)/ws_media.conf.xml"; \
	  else \
	    echo "kept existing config: $(DESTDIR)$(CONF_DIR)/ws_media.conf.xml"; \
	  fi; \
	fi
	@echo ""
	@echo "Next: fs_cli -x 'load mod_ws_media'"

uninstall:
	rm -f "$(DESTDIR)$(MODULES_DIR)/$(TARGET)"
	@echo "removed: $(DESTDIR)$(MODULES_DIR)/$(TARGET) (config left in place)"

clean:
	rm -f $(TARGET) *.o

print-config:
	@echo "CC          = $(CC)"
	@echo "FS_CFLAGS   = $(FS_CFLAGS)"
	@echo "SSL_CFLAGS  = $(SSL_CFLAGS)"
	@echo "SSL_LIBS    = $(SSL_LIBS)"
	@echo "LDSHARED    = $(LDSHARED)"
	@echo "MODULES_DIR = $(MODULES_DIR)"
	@echo "CONF_DIR    = $(CONF_DIR)"

.PHONY: all check-fs install uninstall clean print-config
