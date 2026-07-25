test:
	CI=true \
		node --trace-warnings node_modules/.bin/jest --detectOpenHandles --coverage --colors --runInBand $(TESTARGS)

test-watch:
	CI=true \
		node --trace-warnings node_modules/.bin/jest --detectOpenHandles --coverage --colors --runInBand --watch $(TESTARGS)

build:
	./node_modules/.bin/tsc -p tsconfig.json

lint:
	./node_modules/.bin/eslint '**/*.{js,ts}'

clean:
	rm -rf dist
	rm -f downloads/Qm* downloads/ba*

.PHONY: build test test-watch lint clean
