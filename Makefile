CC=gcc
ODIR:=./obj

DEBUG=-g
FLAG=-std=c99  `mysql_config --cflags --libs`
DEPS =./include/common.h
TARGET=syn_data

INCS=-I./include \
	-I/usr/local/include\
	-I/usr/local/include/json-c\
	-I/usr/local/include/hiredis\
	-I/usr/include \
	-I/usr/pgsql-9.4/include \

LIBS=-L./lib\
	-L/usr/local/lib/ \
	-L/usr/pgsql-9.4/lib \
	-lm \
	-ljson-c\
	-lhiredis\
	-lpq \


CFLAGS=-O2
_DEPS=common.h
DEPS = $(patsubst %,$(IDIR)/%,$(_DEPS))
_OBJ=syndata.o 

OBJ=$(patsubst %,$(ODIR)/%,$(_OBJ))
$(ODIR)/%.o:%.c
	gcc -c  $(INCS)  -o   $@ $< $(CFLAGS) 

all:$(TARGET) ee

$(TARGET):$(OBJ) 
	gcc $(INCS)  -o $@ $^ $(CFLAGS) $(LIBS) $(DEBUG)

ee:
	@echo $(DEPS)
	@echo $(OBJ)
	@echo $(CFLAGS)

clean:
	rm -rf $(OBJ) 
	rm -rf $(TARGET) 
