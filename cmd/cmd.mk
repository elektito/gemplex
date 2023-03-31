BINDIR ?= .
SRC != find . -name '*.go' ! -name '*_test.go'
PKGSRC != find ../../pkg/ -type f -name '*.go' ! -name '*_test.go'

$(BINDIR)/$(NAME): $(SRC) $(PKGSRC)
	go build -o $(BINDIR)

all: $(NAME)

clean:
	rm -f $(BINDIR)/$(NAME)

.PHONY: all clean
