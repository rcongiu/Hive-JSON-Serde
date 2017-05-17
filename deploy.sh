VERSION=`cat pom.xml | grep -oPm1 "(?<=<version>)[^<]+"`

HOST=www.congiu.net
USER=rcongiu

for i in cdh5 hdp23; do 
	echo "Distribution $i version $VERSION "
	DEST_DIR=congiu.net/hive-json-serde/$VERSION/$i/
	ssh $USER@$HOST mkdir -p $DEST_DIR
	# build
	mvn -P$i  clean package || exit -2
	scp json-serde/target/json-serde-${VERSION}-jar-with-dependencies.jar $USER@$HOST:$DEST_DIR || exit -3
        scp json-udf/target/json-udf-${VERSION}-jar-with-dependencies.jar $USER@$HOST:$DEST_DIR || exit -3
	
done
