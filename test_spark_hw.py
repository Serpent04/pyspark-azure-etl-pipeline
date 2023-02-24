import spark_funcs

def test_get_lat():
    #Mansuerto High School, Chicago, IL
    assert spark_funcs.get_lat('2911 W 47th St', 'Chicago', 'US') == 41.807589000932694
    #Belleville West High School, Belleville, IL
    assert spark_funcs.get_lat('4063 Frank Scott Pkwy W', 'Belleville', 'US') == 38.51859800448577
    #Old Traford Stadium, Manchester, UK
    assert spark_funcs.get_lat('Sir Matt Busby Way M16 0RA', 'Manchester', 'UK') == 53.46353459212371
    #None
    assert spark_funcs.get_lat('lklkafslkj', '892398jkjha', 'jkjk964') == None

def test_get_lon():
    #Mansuerto High School, Chicago, IL
    assert spark_funcs.get_lon('2911 W 47th St', 'Chicago', 'US') == -87.6969380340059
    #Belleville West High School, Belleville, IL
    assert spark_funcs.get_lon('4063 Frank Scott Pkwy W', 'Belleville', 'US') == -90.0426530325984
    #Old Traford Stadium, Manchester, UK
    assert spark_funcs.get_lon('Sir Matt Busby Way M16 0RA', 'Manchester', 'UK') == -2.289175051982484
    #None
    assert spark_funcs.get_lon('lklkafslkj', '892398jkjha', 'jkjk964') == None

def test_get_geohash():
    # Mansuerto High School, Chicago, IL
    assert spark_funcs.get_geohash(41.807589000932694, -87.6969380340059) == 'dp3t'
    # Belleville West High School, Belleville, IL
    assert spark_funcs.get_geohash(38.51859800448577, -90.0426530325984) == '9yzg'
    # Old Traford Stadium, Manchester, UK
    assert spark_funcs.get_geohash(53.46353459212371, -2.289175051982484) == 'gcw2'