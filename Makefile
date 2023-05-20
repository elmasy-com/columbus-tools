LDFLAGS = -s
LDFLAGS += -w
LDFLAGS += -extldflags "-static"'

clean:
	@if [ -e "uniquestat/uniquestat" ];	then rm -rf "uniquestat/uniquestat" ; fi
	@if [ -e "duplicator/duplicator" ];	then rm -rf "duplicator/duplicator" ; fi

duplicator:
	cd duplicator && go build -o duplicator -tags netgo -ldflags="$(LDFLAGS)" .

uniquestat:
	cd uniquestat && go build -o uniquestat -tags netgo -ldflags="$(LDFLAGS)" .