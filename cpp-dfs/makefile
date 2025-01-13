# go into each directory and run make, and then symlink the executable from $dir/build/output/app to ../$dir  with the name of $dir

all:
	for dir in dfsc dfsdn dfsnn; do (cd $$dir && make all && mv build/output/app ../build/

clean:
	for dir in dfsc dfsdn dfsnn; do (cd $$dir && make clean && rm -f ../$$dir); done