# taxitrips

## Prerequisities
- Maven 3.6.3
- Java jdk 8
- Apache Storm 2.1.0 (requires python also)

## Query 1 - frequent routes
Query calculates most frequent taxi routes based on 
given data. Output will be written to Query1Output.csv file in execution folder.

### For unix
```
git clone https://github.com/ankiva/taxitrips.git
cd storm
mvn package
storm local --local-ttl 60 target/taxitrips-1.0-SNAPSHOT.jar ee.ut.cs.bigdata.taxitrips.query1.FrequentRoutesTopology "../data/data100.csv"
```

### For windows
```
git clone https://github.com/ankiva/taxitrips.git
cd storm
mvn package
storm.py local --local-ttl 60 target\taxitrips-1.0-SNAPSHOT.jar ee.ut.cs.bigdata.taxitrips.query1.FrequentRoutesTopology "../data/data100.csv"
```

## Query 2 - profitable areas
Query calculates most profitable areas for taxis 
based on given data. 
Output is written to output.csv by default in execution dir(second arg).
### For unix
```
git clone https://github.com/ankiva/taxitrips.git
cd storm
mvn package
storm local --local-ttl 60 target/taxitrips-1.0-SNAPSHOT.jar ee.ut.cs.bigdata.taxitrips.storm.stripes.ProfitableAreasTopology "../data/data100.csv"
```

### For windows
```
git clone https://github.com/ankiva/taxitrips.git
cd storm
mvn package
storm.py local --local-ttl 60 target\taxitrips-1.0-SNAPSHOT.jar ee.ut.cs.bigdata.taxitrips.storm.stripes.ProfitableAreasTopology "../data/data100.csv"
```