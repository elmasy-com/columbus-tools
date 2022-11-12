dump-build:
	cd dump-cli && go build -o dump . && mv dump ..

inspector-build:
	cd inspector-cli && go build -o inspector . && mv inspector ..