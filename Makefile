dump-build:
	cd dump-cli && go build -o dump -ldflags "-s -w" . && mv dump ..

inspector-build:
	cd inspector-cli && go build -o inspector -ldflags "-s -w" . && mv inspector ..