.DEFAULT: build
.PHONY: all clean build install

all: clean build

clean:
	rm -rf build
	rm -rf *.c *.so

build:
	python setup.py build_ext --inplace

install: build
	cp *.so $(_BASE)/lib/python/

test: all
	python test.py

testgdb: all
	gdb -x test_py.gdb python
