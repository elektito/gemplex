TOPTARGETS := all clean install release

BINDIR  != pwd
SUBDIRS != find cmd -mindepth 1 -maxdepth 1 -type d

$(TOPTARGETS): $(SUBDIRS)
$(SUBDIRS):
	$(MAKE) -C $@ BINDIR="$(BINDIR)" $(MAKECMDGOALS)

test:
	go test ./...

.PHONY: $(TOPTARGETS) $(SUBDIRS) test
