__author__ = 'pramod.kumar'
import cartodb as cdb
def insertCartodb(insert):
            user =  'pramod974'
            password =  '23401163'
            CONSUMER_KEY='K0KlgcUfLP3uUOML7RdYMtih9v3ZvG5Ni6El8h4x'
            CONSUMER_SECRET='ZLM07Lpe5GnbPab5vD0OHHk4mTbp8Sq4I1q01LOD'
            cartodb_domain = 'pramod974'
            cl = cdb.CartoDBOAuth(CONSUMER_KEY, CONSUMER_SECRET, user, password, cartodb_domain)
            try:
                x=cl.sql(insert)
                print x
                # print x
            except cdb.CartoDBException as e:
                print "pushing_to_CartoDB~Failed Insert to CartoDB~"+str(e)
                return False
insert="""select * from ra_mansfield  where  cartodb_id in (SELECT MAX(cartodb_id) as cartodb_id FROM
ra_mansfield where  ((padd = '' ) OR ( Terminal_State='RI' or  Terminal_State='SD' or
Terminal_State='ID' or  Terminal_State='MT' or  Terminal_State='OR' ) OR ((
Terminal_State='FL' and Terminal_City='Niceville' ) or( Terminal_State='FL' and
Terminal_City='Pensacola' ) or( Terminal_State='FL' and Terminal_City='Port Tampa' ) or(
Terminal_State='GA' and Terminal_City='Powder Springs' ) or( Terminal_State='GA' and
Terminal_City='Rome' ) or( Terminal_State='MA' and Terminal_City='Everett' ) or(
Terminal_State='MA' and Terminal_City='Revere' ) or( Terminal_State='MA' and
Terminal_City='Springfield' ) or( Terminal_State='MD' and Terminal_City='Salisbury' ) or(
Terminal_State='NC' and Terminal_City='Apex' ) or( Terminal_State='NY' and
Terminal_City='Albany' ) or( Terminal_State='NY' and Terminal_City='Brewerton' ) or(
Terminal_State='NY' and Terminal_City='Glenmont' ) or( Terminal_State='NY' and
Terminal_City='Rochester' ) or( Terminal_State='NY' and Terminal_City='Utica' ) or(
Terminal_State='NY' and Terminal_City='Vestal' ) or( Terminal_State='NY' and
Terminal_City='Warners' ) or( Terminal_State='VA' and Terminal_City='Lorton' ) or(
Terminal_State='IA' and Terminal_City='Dubuque' ) or( Terminal_State='IA' and
Terminal_City='Milford' ) or( Terminal_State='IA' and Terminal_City='Pleasant Hill' ) or(
Terminal_State='IA' and Terminal_City='Riverdale' ) or( Terminal_State='IL' and
Terminal_City='Argo' ) or( Terminal_State='IL' and Terminal_City='Ashkum' ) or(
Terminal_State='IL' and Terminal_City='Cahokia' ) or( Terminal_State='IL' and
Terminal_City='Champaign' ) or( Terminal_State='IL' and Terminal_City='Chillicothe' ) or(
Terminal_State='IL' and Terminal_City='Effingham' ) or( Terminal_State='IL' and
Terminal_City='Forest View' ) or( Terminal_State='IL' and Terminal_City='Forsyth' ) or(
Terminal_State='IL' and Terminal_City='Harristown' ) or( Terminal_State='IL' and
Terminal_City='Kankakee' ) or( Terminal_State='IL' and Terminal_City='Lockport' ) or(
Terminal_State='IL' and Terminal_City='Robinson' ) or( Terminal_State='IL' and
Terminal_City='Rochelle' ) or( Terminal_State='IL' and Terminal_City='Rockford' ) or(
Terminal_State='IN' and Terminal_City='East Chicago' ) or( Terminal_State='IN' and
Terminal_City='Evansville' ) or( Terminal_State='IN' and Terminal_City='Hammond' ) or(
Terminal_State='IN' and Terminal_City='Huntington' ) or( Terminal_State='IN' and
Terminal_City='Mount Vernon' ) or( Terminal_State='IN' and Terminal_City='Muncie' ) or(
Terminal_State='IN' and Terminal_City='Oakland City' ) or( Terminal_State='IN' and
Terminal_City='South Bend' ) or( Terminal_State='IN' and Terminal_City='Whiting' ) or(
Terminal_State='KS' and Terminal_City='Coffeyville' ) or( Terminal_State='KS' and
Terminal_City='Delphos' ) or( Terminal_State='KS' and Terminal_City='Great Bend' ) or(
Terminal_State='KS' and Terminal_City='McPherson' ) or( Terminal_State='KS' and
Terminal_City='Olathe' ) or( Terminal_State='KS' and Terminal_City='Salina' ) or(
Terminal_State='KS' and Terminal_City='Scott City' ) or( Terminal_State='KS' and
Terminal_City='Valley Center' ) or( Terminal_State='KS' and Terminal_City='Wakarusa' ) or(
Terminal_State='KS' and Terminal_City='Wathena' ) or( Terminal_State='KS' and
Terminal_City='Wichita' ) or( Terminal_State='KY' and Terminal_City='Catlettsburg' ) or(
Terminal_State='KY' and Terminal_City='Covington' ) or( Terminal_State='KY' and
Terminal_City='Owensboro' ) or( Terminal_State='KY' and Terminal_City='Paducah' ) or(
Terminal_State='MI' and Terminal_City='Bay City' ) or( Terminal_State='MI' and
Terminal_City='Detroit' ) or( Terminal_State='MI' and Terminal_City='Flint' ) or(
Terminal_State='MI' and Terminal_City='Holland' ) or( Terminal_State='MI' and
Terminal_City='Lansing' ) or( Terminal_State='MI' and Terminal_City='Marshall' ) or(
Terminal_State='MI' and Terminal_City='Mt. Morris' ) or( Terminal_State='MI' and
Terminal_City='North Muskegon' ) or( Terminal_State='MI' and Terminal_City='Novi' ) or(
Terminal_State='MI' and Terminal_City='Owosso' ) or( Terminal_State='MI' and
Terminal_City='River Rouge' ) or( Terminal_State='MI' and Terminal_City='Taylor' ) or(
Terminal_State='MI' and Terminal_City='Woodhaven' ) or( Terminal_State='MN' and
Terminal_City='Alexandria' ) or( Terminal_State='MN' and Terminal_City='Eyota' ) or(
Terminal_State='MN' and Terminal_City='Mankato' ) or( Terminal_State='MN' and
Terminal_City='Marshall' ) or( Terminal_State='MN' and Terminal_City='Moorhead' ) or(
Terminal_State='MN' and Terminal_City='Roseville' ) or( Terminal_State='MN' and
Terminal_City='Sauk Centre' ) or( Terminal_State='MO' and Terminal_City='Cape Girardeau' )
or( Terminal_State='MO' and Terminal_City='Columbia' ) or( Terminal_State='MO' and
Terminal_City='Jefferson City' ) or( Terminal_State='MO' and Terminal_City='Mount Vernon' )
or( Terminal_State='MO' and Terminal_City='Palmyra' ) or( Terminal_State='MO' and
Terminal_City='Scott City' ) or( Terminal_State='MO' and Terminal_City='St. Louis' ) or(
Terminal_State='ND' and Terminal_City='Jamestown' ) or( Terminal_State='ND' and
Terminal_City='Mandan' ) or( Terminal_State='NE' and Terminal_City='Columbus' ) or(
Terminal_State='NE' and Terminal_City='Geneva' ) or( Terminal_State='NE' and
Terminal_City='Norfolk' ) or( Terminal_State='NE' and Terminal_City='North Platte' ) or(
Terminal_State='NE' and Terminal_City='Roca' ) or( Terminal_State='OH' and
Terminal_City='Akron' ) or( Terminal_State='OH' and Terminal_City='Akron/Cleveland' ) or(
Terminal_State='OH' and Terminal_City='Aurora' ) or( Terminal_State='OH' and
Terminal_City='Brecksville' ) or( Terminal_State='OH' and Terminal_City='Canton' ) or(
Terminal_State='OH' and Terminal_City='Cincinnati' ) or( Terminal_State='OH' and
Terminal_City='Cleveland' ) or( Terminal_State='OH' and Terminal_City='Columbus' ) or(
Terminal_State='OH' and Terminal_City='Cuyahoga Hts.' ) or( Terminal_State='OH' and
Terminal_City='Dublin' ) or( Terminal_State='OH' and Terminal_City='Heath' ) or(
Terminal_State='OH' and Terminal_City='Lebanon' ) or( Terminal_State='OH' and
Terminal_City='Lima' ) or( Terminal_State='OH' and Terminal_City='Marietta' ) or(
Terminal_State='OH' and Terminal_City='Oregon' ) or( Terminal_State='OH' and
Terminal_City='Steubenville' ) or( Terminal_State='OH' and Terminal_City='Toledo' ) or(
Terminal_State='OH' and Terminal_City='Youngstown' ) or( Terminal_State='OK' and
Terminal_City='Ardmore' ) or( Terminal_State='OK' and Terminal_City='Enid' ) or(
Terminal_State='OK' and Terminal_City='Ponca City' ) or( Terminal_State='WI' and
Terminal_City='Green Bay' ) or( Terminal_State='WI' and Terminal_City='McFarland' ) or(
Terminal_State='AL' and Terminal_City='Saraland' ) or( Terminal_State='AR' and
Terminal_City='Bono' ) or( Terminal_State='AR' and Terminal_City='El Dorado' ) or(
Terminal_State='AR' and Terminal_City='North Little Rock' ) or( Terminal_State='AR' and
Terminal_City='West Memphis' ) or( Terminal_State='LA' and Terminal_City='Baton Rouge' ) or(
Terminal_State='LA' and Terminal_City='Chalmette' ) or( Terminal_State='LA' and
Terminal_City='Jonesville' ) or( Terminal_State='LA' and Terminal_City='Meraux' ) or(
Terminal_State='LA' and Terminal_City='Monroe' ) or( Terminal_State='LA' and
Terminal_City='Opelousas' ) or( Terminal_State='LA' and Terminal_City='Port Allen' ) or(
Terminal_State='LA' and Terminal_City='Westlake' ) or( Terminal_State='MS' and
Terminal_City='Vicksburg' ) or( Terminal_State='TX' and Terminal_City='Abernathy' ) or(
Terminal_State='TX' and Terminal_City='Aledo' ) or( Terminal_State='TX' and
Terminal_City='Amarillo' ) or( Terminal_State='TX' and Terminal_City='Beaumont' ) or(
Terminal_State='TX' and Terminal_City='Big Sandy' ) or( Terminal_State='TX' and
Terminal_City='Brownsville' ) or( Terminal_State='TX' and Terminal_City='Buda' ) or(
Terminal_State='TX' and Terminal_City='Caddo Mills' ) or( Terminal_State='TX' and
Terminal_City='Center' ) or( Terminal_State='TX' and Terminal_City='Corpus Christi' ) or(
Terminal_State='TX' and Terminal_City='Edinburg' ) or( Terminal_State='TX' and
Terminal_City='Euless' ) or( Terminal_State='TX' and Terminal_City='Harlingen' ) or(
Terminal_State='TX' and Terminal_City='Hearne' ) or( Terminal_State='TX' and
Terminal_City='Irving' ) or( Terminal_State='TX' and Terminal_City='Laredo' ) or(
Terminal_State='TX' and Terminal_City='Lubbock' ) or( Terminal_State='TX' and
Terminal_City='Mertens ' ) or( Terminal_State='TX' and Terminal_City='Mount Pleasant' ) or(
Terminal_State='TX' and Terminal_City='Odessa' ) or( Terminal_State='TX' and
Terminal_City='Placedo' ) or( Terminal_State='TX' and Terminal_City='Sunray' ) or(
Terminal_State='TX' and Terminal_City='Three Rivers' ) or( Terminal_State='TX' and
Terminal_City='Waskom' ) or( Terminal_State='TX' and Terminal_City='Wichita Falls' ) or(
Terminal_State='CO' and Terminal_City='Colorado Springs' ) or( Terminal_State='CA' and
Terminal_City='Anaheim' ) or( Terminal_State='CA' and Terminal_City='Benicia' ) or(
Terminal_State='CA' and Terminal_City='Carson' ) or( Terminal_State='CA' and
Terminal_City='Daggett' ) or( Terminal_State='CA' and Terminal_City='El Segundo' ) or(
Terminal_State='CA' and Terminal_City='Imperial' ) or( Terminal_State='CA' and
Terminal_City='Los Angeles' ) or( Terminal_State='CA' and Terminal_City='Martinez' ) or(
Terminal_State='CA' and Terminal_City='Orange' ) or( Terminal_State='CA' and
Terminal_City='Rancho Cordova' ) or( Terminal_State='CA' and Terminal_City='Richmond' ) or(
Terminal_State='CA' and Terminal_City='Tracy' ) or( Terminal_State='CA' and
Terminal_City='Vernon' ) or( Terminal_State='CA' and Terminal_City='West Sacramento' ) or(
Terminal_State='CA' and Terminal_City='Wilmington' ) or( Terminal_State='CA' and
Terminal_City='NoCal' ) or( Terminal_State='CA' and Terminal_City='SoCal' ) or(
Terminal_State='NV' and Terminal_City='Las Vegas' ) or( Terminal_State='WA' and
Terminal_City='Blaine' ) or ( Terminal_State='WA' and Terminal_City='Ferndale' ) or(
Terminal_State='WA' and Terminal_City='Pasco' ) or( Terminal_State='WA' and
Terminal_City='Renton' ) or( Terminal_State='WA' and Terminal_City='Seattle' ) or(
Terminal_State='WA' and Terminal_City='Spokane' ) or( Terminal_State='WA' and
Terminal_City='Tacoma')) OR ( en_terminal_name='CT New Haven - Mag - 1265' or
en_terminal_name='CT New Haven - Waterfront - 1264' or  en_terminal_name='FL Fort Lauderdale
- Exxon - 2161' or  en_terminal_name='FL Fort Lauderdale - MPC - 2160' or
en_terminal_name='FL Fort Lauderdale - MPC - 2163' or  en_terminal_name='FL Tampa - MPC -
2136' or  en_terminal_name='FL Tampa - Murphy - 2100' or  en_terminal_name='FL Tampa -
Motiva - 2124' or  en_terminal_name='FL Tampa - Chevron - 2131' or  en_terminal_name='GA
Albany - TMG - 2502' or  en_terminal_name='GA Columbus - MPC - 2523' or
en_terminal_name='GA Columbus - Omega - 2522' or  en_terminal_name='GA Doraville - Chevron -
2528' or  en_terminal_name='GA Doraville - Motiva - 2531' or  en_terminal_name='GA Doraville
- MPC - 2532' or  en_terminal_name='GA Macon - MPC - 2541' or  en_terminal_name='GA Macon -
TMG - 2544' or  en_terminal_name='NC Selma - KM - 2033' or  en_terminal_name='NC Selma - MPC
- 2029' or  en_terminal_name='NC Selma - TMG - 2028' or  en_terminal_name='NJ Linden - P66 -
1512' or  en_terminal_name='NJ Linden - P66 - 1514' or  en_terminal_name='NJ Paulsboro -
Plains - 1525' or  en_terminal_name='NY Lawrence - Carbo - 1324' or  en_terminal_name='PA
Coraopolis - Buckeye - 1792' or  en_terminal_name='PA Macungie - Buckeye - 1700' or
en_terminal_name='PA Midland - MPC - 1773' or  en_terminal_name='PA Philadelphia - Plains -
1734' or  en_terminal_name='PA Pittsburgh - Sunoco - 1781' or  en_terminal_name='PA
Whitehall - Sunoco - 1711' or  en_terminal_name='SC Belton - MPC - 2053' or
en_terminal_name='SC North Augusta - Buckeye - 2061' or  en_terminal_name='SC Spartanburg -
Citgo - 2077' or  en_terminal_name='SC Spartanburg - Motiva - 2075' or  en_terminal_name='VA
Fairfax - Motiva - 1662' or  en_terminal_name='VA Fairfax - TMG - 1660' or
en_terminal_name='VA Montvale - Buckeye - 1665' or  en_terminal_name='VA Montvale - TMG -
1666' or  en_terminal_name='VA Richmond - Mag - 1684' or  en_terminal_name='VA Richmond -
Motiva - 1685' or  en_terminal_name='VA Richmond - TMG - 1678' or  en_terminal_name='VA
Roanoke - KM - 1688' or  en_terminal_name='VA Roanoke - Mag - 1689' or  en_terminal_name='IL
Arlington Heights - Exxon - 3311' or  en_terminal_name='IL Arlington Heights - MPC - 3307'
or  en_terminal_name='IL Hartford - P66 - 3353' or  en_terminal_name='IN Indianapolis - BP -
3204' or  en_terminal_name='IN Indianapolis - Buckeye - 3226' or  en_terminal_name='IN
Indianapolis - Buckeye - 3230' or  en_terminal_name='IN Indianapolis - MPC - 3219' or
en_terminal_name='IN Indianapolis - MPC - 3222' or  en_terminal_name='KS Kansas City - P66 -
3672' or  en_terminal_name='KY Lexington - MPC - 3266' or  en_terminal_name='KY Louisville -
MPC - 3268' or  en_terminal_name='KY Louisville - MPC - 3272' or  en_terminal_name='MI
Ferrysburg - Buckeye - 3013' or  en_terminal_name='MI Niles - Buckeye - 3028' or
en_terminal_name='MI Niles - MPC - 3019' or  en_terminal_name='MI Romulus - Sonoco - 3037'
or  en_terminal_name='OH Dayton - BP - 3106' or  en_terminal_name='OH Dayton - Buckeye -
3115' or  en_terminal_name='OH Dayton - Citgo - 3121' or  en_terminal_name='OH Dayton -
Sunoco - 3117' or  en_terminal_name='TN Chattanooga - Citgo - 2202' or  en_terminal_name='TN
Chattanooga - Mag - 2200' or  en_terminal_name='TN Knoxville - Citgo - 2213' or
en_terminal_name='TN Knoxville - MPC - 2217' or  en_terminal_name='TN Memphis - Exxon -
2225' or  en_terminal_name='TN Nashville - Citgo - 2233' or  en_terminal_name='TN Nashville
- MPC - 2232' or  en_terminal_name='TN Nashville - MPC - 2238' or  en_terminal_name='WI
Milwaukee - MPC - 3076' or  en_terminal_name='AL Birmingham - Motiva - 2308' or
en_terminal_name='AL Birmingham - MPC - 2306' or  en_terminal_name='AL Montgomery - Epic -
2326' or  en_terminal_name='AL Montgomery - Mag - 2322' or  en_terminal_name='AL Montgomery
- MPC - 2325' or  en_terminal_name='AL Montgomery - Murphy - 2327' or  en_terminal_name='LA
Arcadia - Sunoco - 2353' or  en_terminal_name='MS Greenville - Delta - 2421' or
en_terminal_name='MS Meridian - Citgo - 2412' or  en_terminal_name='MS Meridian - Murphy -
2414' or  en_terminal_name='NM Albuquerque - Nustar - 4253' or  en_terminal_name='NM
Albuquerque - P66 - 4254' or  en_terminal_name='TX Dallas - Mag - 2661' or
en_terminal_name='TX Dallas - Motiva - 2662' or  en_terminal_name='TX El Paso - Mag - 2751'
or  en_terminal_name='TX Grapevine - Mag - 2671' or  en_terminal_name='TX Houston - ERPC -
2817' or  en_terminal_name='TX Pasadena - KM - 2830' or  en_terminal_name='TX Pasadena - P66
- 2811' or  en_terminal_name='TX San Antonio - Exxon - 2740' or  en_terminal_name='CO
Commerce City - P66 - 4104' or  en_terminal_name='UT Salt Lake City - Tesoro - 4202' or
en_terminal_name='AZ Phoenix - Unknown - G002' or  en_terminal_name='CA Rialto - P66 - 4760'
or  en_terminal_name='CA Sacramento - Chevron - 4621' or  en_terminal_name='CA Sacramento -
P66 - 4624' or  en_terminal_name='CA San Diego - Chevron - 4773' or  en_terminal_name='CA
San Diego - SFPP - 4776' or  en_terminal_name='CA San Jose - Chevron - 4650' or
en_terminal_name='CA San Jose - SFPP - 4652' or  en_terminal_name='CA San Jose - Shell -
4653' or  en_terminal_name='CA Stockton - Nustar - 4626' or  en_terminal_name='CA Stockton -
Shell - 4628' or  en_terminal_name='CA Stockton - Tesoro - 4629' or  en_terminal_name='WA
Anacortes - Shell - 4400' or en_terminal_name='WA Anacortes - Tesoro - 4428' ) OR ((
en_terminal_name='DE Delaware City - Del City - 1600' and supplier_name='Shell' ) or(
en_terminal_name='DE Delaware City - Del City - 1600' and supplier_name='Valero' ) or(
en_terminal_name='FL Cape Canaveral - TMG - 2138' and supplier_name='TMG' ) or(
en_terminal_name='FL Fort Lauderdale - BP - 2152' and supplier_name='BP' ) or(
en_terminal_name='FL Jacksonville - MPC - 2106' and supplier_name='BP' ) or(
en_terminal_name='FL Jacksonville - MPC - 2106' and supplier_name='MPC' ) or(
en_terminal_name='FL Orlando - CFPL - 2129' and supplier_name='MPC' ) or(
en_terminal_name='FL Orlando - CFPL - 2129' and supplier_name='Murphy' ) or(
en_terminal_name='FL Panama City - Chevron - 2116' and supplier_name='Citgo' ) or(
en_terminal_name='FL Panama City - Chevron - 2116' and supplier_name='Valero' ) or(
en_terminal_name='FL Tampa - Citgo - 2133' and supplier_name='Valero' ) or(
en_terminal_name='FL Tampa - KM - 2123' and supplier_name='Exxon' ) or( en_terminal_name='FL
Tampa - KM - 2123' and supplier_name='Murphy' ) or( en_terminal_name='FL Tampa - TMG - 2101'
and supplier_name='TMG' ) or( en_terminal_name='GA Albany - KM - 2500' and
supplier_name='MPC' ) or( en_terminal_name='GA Albany - Mag - 2501' and
supplier_name='Murphy' ) or( en_terminal_name='GA Athens - KM - 2506' and
supplier_name='MPC' ) or( en_terminal_name='GA Bainbridge - Motiva - 2514' and
supplier_name='Citgo' ) or( en_terminal_name='GA Bainbridge - Motiva - 2514' and
supplier_name='MPC' ) or( en_terminal_name='GA Bainbridge - Motiva - 2514' and
supplier_name='Shell' ) or( en_terminal_name='GA Bainbridge - TMG - 2515' and
supplier_name='TMG' ) or( en_terminal_name='GA Doraville - Mag - 2533' and
supplier_name='Murphy' ) or( en_terminal_name='GA Doraville - Motiva - 2510' and
supplier_name='Shell' ) or( en_terminal_name='GA Macon - Mag - 2543' and
supplier_name='Murphy' ) or( en_terminal_name='GA Macon - Vecenergy - 2538' and
supplier_name='Placid' ) or( en_terminal_name='MD Baltimore - BP - 1551' and
supplier_name='Exxon' ) or( en_terminal_name='MD Baltimore - Citgo - 1562' and
supplier_name='Citgo' ) or( en_terminal_name='MD Baltimore - Motiva - 1561' and
supplier_name='Gulf' ) or( en_terminal_name='MD Baltimore - Motiva - 1561' and
supplier_name='Valero' ) or( en_terminal_name='MD Baltimore - Sunoco - 1552' and
supplier_name='Valero' ) or( en_terminal_name='NC Charlotte - KM - 2000' and
supplier_name='Exxon' ) or( en_terminal_name='NC Charlotte - Mag - 2006' and
supplier_name='Murphy' ) or( en_terminal_name='NC Charlotte - Mag - 2006' and
supplier_name='Valero' ) or( en_terminal_name='NC Charlotte - Mag - 2024' and
supplier_name='Murphy' ) or( en_terminal_name='NC Charlotte - Mag - 2024' and
supplier_name='Valero' ) or( en_terminal_name='NC Charlotte - MPC - 2002' and
supplier_name='MPC' ) or( en_terminal_name='NC Charlotte - MPC - 2008' and
supplier_name='MPC' ) or( en_terminal_name='NC Greensboro - KM - 2015' and
supplier_name='MPC' ) or( en_terminal_name='NC Greensboro - Mag - 2011' and
supplier_name='Murphy' ) or( en_terminal_name='NC Greensboro - Mag - 2020' and
supplier_name='Murphy' ) or( en_terminal_name='NC Greensboro - Mag - 2020' and
supplier_name='P66' ) or( en_terminal_name='NC Selma - Mag - 2036' and supplier_name='MPC' )
or( en_terminal_name='NC Selma - Mag - 2036' and supplier_name='Murphy' ) or(
en_terminal_name='NJ Linden - Citgo - 1513' and supplier_name='Citgo' ) or(
en_terminal_name='NJ Linden - Gulf - 1515' and supplier_name='Citgo' ) or(
en_terminal_name='NJ Paulsboro - Nustar - 1526' and supplier_name='Valero' ) or(
en_terminal_name='NY Lawrence - Motiva - 1312' and supplier_name='Valero' ) or(
en_terminal_name='NY New Windsor - Global - 1411' and supplier_name='Citgo' ) or(
en_terminal_name='NY New Windsor - Global - 1411' and supplier_name='P66' ) or(
en_terminal_name='NY New Windsor - Global - 1413' and supplier_name='Citgo' ) or(
en_terminal_name='NY New Windsor - Global - 1413' and supplier_name='Exxon' ) or(
en_terminal_name='NY New Windsor - Global - 1413' and supplier_name='Valero' ) or(
en_terminal_name='PA Coraopolis - PPC - 1780' and supplier_name='PBF' ) or(
en_terminal_name='PA Coraopolis - Pyramid - 1780' and supplier_name='MPC' ) or(
en_terminal_name='PA Coraopolis - Pyramid - 1780' and supplier_name='PBF' ) or(
en_terminal_name='PA Delmont - Sunoco - 1761' and supplier_name='Exxon' ) or(
en_terminal_name='PA Mechanicsburg - PPC - 1715' and supplier_name='PBF' ) or(
en_terminal_name='PA Mechanicsburg - Pyramid - 1715' and supplier_name='PBF' ) or(
en_terminal_name='PA Middletown - PPC - 1716' and supplier_name='PBF' ) or(
en_terminal_name='PA Middletown - Pyramid - 1716' and supplier_name='PBF' ) or(
en_terminal_name='PA Middletown - Pyramid - 1716' and supplier_name='Shell' ) or(
en_terminal_name='PA Middletown - Pyramid - 1716' and supplier_name='Valero' ) or(
en_terminal_name='PA Pittsburgh - PPC - 1776' and supplier_name='Husky' ) or(
en_terminal_name='PA Pittsburgh - PPC - 1776' and supplier_name='PBF' ) or(
en_terminal_name='PA Pittsburgh - Pyramid - 1776' and supplier_name='Husky' ) or(
en_terminal_name='PA Pittsburgh - Pyramid - 1776' and supplier_name='PBF' ) or(
en_terminal_name='PA Sinking Spring - PPC - 1742' and supplier_name='Valero' ) or(
en_terminal_name='PA Sinking Spring - Pyramid - 1742' and supplier_name='Valero' ) or(
en_terminal_name='SC Belton - Buckeye - 2051' and supplier_name='Placid' ) or(
en_terminal_name='SC North Augusta - KM - 2060' and supplier_name='MPC' ) or(
en_terminal_name='SC North Augusta - KM - 2062' and supplier_name='Gulf' ) or(
en_terminal_name='SC North Augusta - KM - 2062' and supplier_name='Placid' ) or(
en_terminal_name='SC North Augusta - KM - 2062' and supplier_name='TMG' ) or(
en_terminal_name='SC North Augusta - Mag - 2059' and supplier_name='Murphy' ) or(
en_terminal_name='SC North Augusta - Mag - 2063' and supplier_name='Murphy' ) or(
en_terminal_name='SC North Augusta - Mag - 2063' and supplier_name='TMG' ) or(
en_terminal_name='SC Spartanburg - Mag - 2076' and supplier_name='Murphy' ) or(
en_terminal_name='SC Spartanburg - Buckeye - 2052' and supplier_name='Placid' ) or(
en_terminal_name='SC Spartanburg - Mag - 2068' and supplier_name='Murphy' ) or(
en_terminal_name='VA Chesapeake - Buckeye - 1650' and supplier_name='BP' ) or(
en_terminal_name='VA Chesapeake - KM - 1654' and supplier_name='Valero' ) or(
en_terminal_name='VA Montvale - Mag - 1668' and supplier_name='MPC' ) or(
en_terminal_name='VA Montvale - Mag - 1668' and supplier_name='Murphy' ) or(
en_terminal_name='VA Montvale - Mag - 1668' and supplier_name='Valero' ) or(
en_terminal_name='VA Newington - KM - 1671' and supplier_name='Exxon' ) or(
en_terminal_name='VA Newington - KM - 1671' and supplier_name='TMG' ) or(
en_terminal_name='VA Richmond - Citgo - 1679' and supplier_name='Murphy' ) or(
en_terminal_name='VA Richmond - KM - 1681' and supplier_name='P66' ) or(
en_terminal_name='VA Roanoke - KM - 1690' and supplier_name='Exxon' ) or(
en_terminal_name='VA Roanoke - KM - 1690' and supplier_name='MPC' ) or( en_terminal_name='WV
Charleston - MPC - 3181' and supplier_name='MPC' ) or( en_terminal_name='IA Coralville - Mag
- 3463' and supplier_name='Murphy' ) or( en_terminal_name='IA Des Moines - Mag - 3457' and
supplier_name='Murphy' ) or( en_terminal_name='IA Waterloo - Mag - 3474' and
supplier_name='Murphy' ) or( en_terminal_name='IA Waterloo - Mag - 3474' and
supplier_name='Valero' ) or( en_terminal_name='IL Hartford - BP - 3351' and
supplier_name='P66' ) or( en_terminal_name='IL Lemont - Citgo - 3317' and
supplier_name='Valero' ) or( en_terminal_name='KS Kansas City - Mag - 3659' and
supplier_name='Valero' ) or( en_terminal_name='MI Ferrysburg - Citgo - 3008' and
supplier_name='Murphy' ) or( en_terminal_name='MI Niles - Citgo - 3010' and
supplier_name='Murphy' ) or( en_terminal_name='MI Romulus - MPC - 3034' and
supplier_name='MPC' ) or( en_terminal_name='MO Brookline - Mag - 3718' and
supplier_name='P66' ) or( en_terminal_name='NE Doniphan - Mag - 3602' and
supplier_name='Murphy' ) or( en_terminal_name='NE Omaha - Mag - 3608' and
supplier_name='Murphy' ) or( en_terminal_name='NE Omaha - Mag - 3608' and
supplier_name='P66' ) or( en_terminal_name='OK Oklahoma City - Mag - 2613' and
supplier_name='Valero' ) or( en_terminal_name='OK Oklahoma City - P66 - 2612' and
supplier_name='P66' ) or( en_terminal_name='OK Tulsa - Mag - 2622' and
supplier_name='Valero' ) or( en_terminal_name='TN Chattanooga - Mag - 2208' and
supplier_name='Murphy' ) or( en_terminal_name='TN Knoxville - Cummins - 2214' and
supplier_name='Placid' ) or( en_terminal_name='TN Knoxville - Mag - 2219' and
supplier_name='Murphy' ) or( en_terminal_name='TN Memphis - Valero - 2227' and
supplier_name='Citgo' ) or( en_terminal_name='TN Nashville - Mag - 2240' and
supplier_name='Placid' ) or( en_terminal_name='TN Nashville - MPC - 2237' and
supplier_name='MPC' ) or( en_terminal_name='AR Fort Smith - Mag - 2453' and
supplier_name='Murphy' ) or( en_terminal_name='LA Arcadia - Chevron - 2351' and
supplier_name='Valero' ) or( en_terminal_name='LA Bossier City - ERPC - 2394' and
supplier_name='Valero' ) or( en_terminal_name='LA Convent - Motiva - 2361' and
supplier_name='Valero' ) or( en_terminal_name='LA Kenner - Motiva - 2365' and
supplier_name='Placid' ) or( en_terminal_name='MS Collins - Chevron - 2401' and
supplier_name='Placid' ) or( en_terminal_name='MS Collins - KM - 2402' and
supplier_name='MPC' ) or( en_terminal_name='MS Collins - KM - 2402' and supplier_name='P66'
) or( en_terminal_name='MS Collins - KM - 2402' and supplier_name='Placid' ) or(
en_terminal_name='MS Collins - TMG - 2405' and supplier_name='P66' ) or(
en_terminal_name='MS Collins - TMG - 2405' and supplier_name='TMG' ) or(
en_terminal_name='MS Meridian - MGC - 2411' and supplier_name='P66' ) or(
en_terminal_name='MS Pascagoula - Chevron - 2416' and supplier_name='Placid' ) or(
en_terminal_name='MS Pascagoula - Chevron - 2416' and supplier_name='Valero' ) or(
en_terminal_name='TX Fort Worth - Chevron - 2666' and supplier_name='Valero' ) or(
en_terminal_name='TX Houston - Mag - 2831' and supplier_name='Valero' ) or(
en_terminal_name='CO Aurora - Mag - 4100' and supplier_name='Shell' ) or(
en_terminal_name='CO Aurora - Mag - 4100' and supplier_name='Valero' ) or(
en_terminal_name='CO DuPont - Mag - 4105' and supplier_name='Shell' ) or(
en_terminal_name='AZ Phoenix - SFPP - 4304' and supplier_name='Chevron' ) or(
en_terminal_name='AZ Phoenix - SFPP - 4304' and supplier_name='P66' ) or(
en_terminal_name='CA Colton - SFPP - 4757' and supplier_name='Chevron' ) or(
en_terminal_name='CA Colton - SFPP - 4757' and supplier_name='Exxon' ) or(
en_terminal_name='CA Colton - SFPP - 4757' and supplier_name='P66' ) or(
en_terminal_name='CA Colton - Tesoro - 4753' and supplier_name='Tesoro' ) or(
en_terminal_name='CA Brisbane - SFPP - 4700' and supplier_name='P66' ) or(
en_terminal_name='CA Brisbane - SFPP - 4700' and supplier_name='Tesoro' ) or(
en_terminal_name='CA Fresno - SFPP - 4651' and supplier_name='P66' ) or(
en_terminal_name='CA Fresno - SFPP - 4651' and supplier_name='Shell' ) or(
en_terminal_name='CA Fresno - SFPP - 4651' and supplier_name='Tesoro' ) or(
en_terminal_name='CA Fresno - SFPP - 4651' and supplier_name='Valero' ) or(
en_terminal_name='CA Long Beach - Tesoro - 4764' and supplier_name='Tesoro' ) or(
en_terminal_name='CA San Diego - Tesoro - 4782' and supplier_name='Tesoro' ) or(
en_terminal_name='CA South Gate - Tesoro - 4807' and supplier_name='Tesoro' ) or(
en_terminal_name='NV Sparks - SFPP - 4353' and supplier_name='P66' ) or(
en_terminal_name='NV Sparks - SFPP - 4353' and supplier_name='Tesoro' ) or(
en_terminal_name='0Unknown' and supplier_name='MPC' ) or( en_terminal_name='0Unknown' and
supplier_name='Tesoro' ) or( en_terminal_name='0Unknown' and supplier_name='Gulf' ) or(
en_terminal_name='0Unknown' and supplier_name='PBF' ) or( en_terminal_name='0Unknown' and
supplier_name='TMG' ) or( en_terminal_name='0Unknown' and supplier_name='Murphy')) OR ((
en_terminal_name='CT New Haven - Motiva - 1254' and supplier_name='Valero' and
Account_Type='UNB Spot' ) or( en_terminal_name='FL Fort Lauderdale - Citgo - 2157' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE-222307' ) or(
en_terminal_name='FL Orlando - CFPL - 2129' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE-222307' ) or( en_terminal_name='FL Orlando -
CFPL - 2129' and supplier_name='Valero' and Account_Type='UNB Spot' ) or(
en_terminal_name='FL Tampa - Citgo - 2133' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE-222307' ) or( en_terminal_name='FL Tampa - KM
- 2123' and supplier_name='Valero' and Account_Type='UNB Spot' ) or( en_terminal_name='GA
Albany - KM - 2500' and supplier_name='Valero' and Account_Type='UNB Spot' ) or(
en_terminal_name='GA Athens - KM - 2506' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE-222307' ) or( en_terminal_name='GA Athens - KM
- 2506' and supplier_name='Valero' and Account_Type='UNB Spot' ) or( en_terminal_name='GA
Bainbridge - TMG - 2515' and supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF
GAINESVILLE-222307' ) or( en_terminal_name='GA Columbus - KM - 2520' and
supplier_name='Chevron' and Account_Type='MANSFIELD OIL COMPANY : AC5246494' ) or(
en_terminal_name='GA Columbus - KM - 2520' and supplier_name='Chevron' and
Account_Type='MANSFIELD OIL COMPANY : CHV5246494' ) or( en_terminal_name='GA Doraville -
Citgo - 2529' and supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--
224531' ) or( en_terminal_name='GA Doraville - Citgo - 2529' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE-222307' ) or( en_terminal_name='GA Doraville -
Citgo - 2529' and supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO/MYSTIK-376859' )
or( en_terminal_name='GA Doraville - Citgo - 2529' and supplier_name='Valero' and
Account_Type='UNB Spot' ) or( en_terminal_name='MA Braintree - Citgo - 1155' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO-FORWARD PRICE O-342837' ) or(
en_terminal_name='MD Baltimore - BP - 1551' and supplier_name='BP' and Account_Type='50 -
Commercial' ) or( en_terminal_name='MD Baltimore - BP - 1551' and supplier_name='BP' and
Account_Type='Channel: 50 - Commercial' ) or( en_terminal_name='MD Baltimore - Citgo - 1562'
and supplier_name='TMG' and Account_Type='MANSFIELD OIL COMPANY-ESC (5738700)' ) or(
en_terminal_name='MD Baltimore - Citgo - 1562' and supplier_name='Valero' and
Account_Type='Unknown' ) or( en_terminal_name='NC Charlotte - Citgo - 2001' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE-222307' ) or(
en_terminal_name='NC Charlotte - KM - 2000' and supplier_name='Valero' and Account_Type='UNB
Spot' ) or( en_terminal_name='NC Greensboro - KM - 2014' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL GAIN - UNBR FXD GBORO DSL' ) or( en_terminal_name='NC Greensboro
- KM - 2014' and supplier_name='Valero' and Account_Type='UNB Spot' ) or(
en_terminal_name='NC Greensboro - KM - 2015' and supplier_name='Valero' and
Account_Type='UNB Spot' ) or( en_terminal_name='NC Greensboro - Mag - 2011' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE-222307' ) or(
en_terminal_name='NC Greensboro - Mag - 2020' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE-222307' ) or( en_terminal_name='NC Selma -
Citgo - 2030' and supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE-
222307' ) or( en_terminal_name='NC Selma - KM - 2013' and supplier_name='Valero' and
Account_Type='UNB Spot' ) or( en_terminal_name='NC Selma - KM - 2018' and
supplier_name='Valero' and Account_Type='UNB Spot' ) or( en_terminal_name='NJ Linden - Citgo
- 1513' and supplier_name='Valero' and Account_Type='UNB Spot' ) or( en_terminal_name='NJ
Newark - Motiva - 1521' and supplier_name='Valero' and Account_Type='UNB Spot' ) or(
en_terminal_name='NJ Sewaren - Motiva - 1538' and supplier_name='Valero' and
Account_Type='UNB Spot' ) or( en_terminal_name='NY New Windsor - Global - 1411' and
supplier_name='Valero' and Account_Type='UNB Spot' ) or( en_terminal_name='PA Coraopolis -
Pyramid - 1780' and supplier_name='Shell' and Account_Type='MANSFIELD OIL COMPANY OF
12313997' ) or( en_terminal_name='PA Mechanicsburg - Pyramid - 1715' and
supplier_name='Shell' and Account_Type='MANSFIELD OIL COMPANY OF 12313997' ) or(
en_terminal_name='PA Middletown - PPC - 1716' and supplier_name='Valero' and
Account_Type='UNB Spot' ) or( en_terminal_name='PA Philadelphia - Plains - 1737' and
supplier_name='Valero' and Account_Type='UNB Spot' ) or( en_terminal_name='PA Pittsburgh -
Gulf - 1777' and supplier_name='Valero' and Account_Type='UNB Spot' ) or(
en_terminal_name='PA Pittsburgh - PPC - 1776' and supplier_name='Valero' and
Account_Type='UNB Spot' ) or( en_terminal_name='PA Pittsburgh - Pyramid - 1776' and
supplier_name='Valero' and Account_Type='UNB Spot' ) or( en_terminal_name='PA Pittston - PPC
- 1707' and supplier_name='Valero' and Account_Type='UNB Spot' ) or( en_terminal_name='PA
Pittston - Pyramid - 1707' and supplier_name='Valero' and Account_Type='UNB Spot' ) or(
en_terminal_name='SC North Augusta - KM - 2060' and supplier_name='Valero' and
Account_Type='UNB Spot' ) or( en_terminal_name='SC North Augusta - Mag - 2063' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE-222307' ) or(
en_terminal_name='SC North Charleston - Buckeye - 2064' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE-222307' ) or( en_terminal_name='SC Spartanburg
- Mag - 2076' and supplier_name='Valero' and Account_Type='UNB Spot' ) or(
en_terminal_name='SC Spartanburg - KM - 2074' and supplier_name='Valero' and
Account_Type='UNB Spot' ) or( en_terminal_name='VA Chesapeake - Buckeye - 1650' and
supplier_name='Valero' and Account_Type='UNB Spot' ) or( en_terminal_name='VA Chesapeake -
Citgo - 1652' and supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE-
222307' ) or( en_terminal_name='VA Chesapeake - Citgo - 1652' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO-FORWARD PRICE O-342837' ) or( en_terminal_name='VA Chesapeake
- Citgo - 1652' and supplier_name='Valero' and Account_Type='UNB Spot' ) or(
en_terminal_name='VA Fairfax - Buckeye - 1659' and supplier_name='Valero' and
Account_Type='UNB Spot' ) or( en_terminal_name='VA Fairfax - Citgo - 1661' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' ) or(
en_terminal_name='VA Fairfax - Citgo - 1661' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE-222307' ) or( en_terminal_name='VA Fairfax -
Citgo - 1661' and supplier_name='Valero' and Account_Type='UNB Spot' ) or(
en_terminal_name='VA Newington - KM - 1671' and supplier_name='Valero' and Account_Type='UNB
Spot' ) or( en_terminal_name='VA Richmond - Buckeye - 1677' and supplier_name='Valero' and
Account_Type='UNB Spot' ) or( en_terminal_name='VA Richmond - Citgo - 1679' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE-222307' ) or(
en_terminal_name='VA Richmond - Citgo - 1679' and supplier_name='Valero' and
Account_Type='UNB Spot' ) or( en_terminal_name='VA Richmond - KM - 1681' and
supplier_name='Valero' and Account_Type='UNB Spot' ) or( en_terminal_name='VA Roanoke - KM -
1690' and supplier_name='Valero' and Account_Type='UNB Spot' ) or( en_terminal_name='WV
Charleston - MPC - 3181' and supplier_name='Exxon' and Account_Type='MANSFIELD OIL COMPANY
OF 106305 IW' ) or( en_terminal_name='IL Alsip - Valero - 3300' and supplier_name='Valero'
and Account_Type='UNB Spot' ) or( en_terminal_name='IL Arlington Heights - Citgo - 3304' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO-383369' ) or( en_terminal_name='IL
Arlington Heights - Citgo - 3318' and supplier_name='Citgo' and Account_Type='MANSFIELD OIL
CO / CTA ONLY-362654' ) or( en_terminal_name='IL Arlington Heights - Citgo - 3318' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO-383369' ) or( en_terminal_name='IL
Hartford - BP - 3351' and supplier_name='BP' and Account_Type='Channel: 50 - Commercial' )
or( en_terminal_name='IL Lemont - Citgo - 3317' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO / CTA ONLY-362654' ) or( en_terminal_name='IL Lemont - Citgo
- 3317' and supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO-383369' ) or(
en_terminal_name='IL Lemont - Citgo - 3317' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO-FORWARD PRICE O-342837' ) or( en_terminal_name='IN
Indianapolis - Buckeye - 3238' and supplier_name='Valero' and Account_Type='UNB Spot' ) or(
en_terminal_name='KY Louisville - Valero - 3270' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Contract' ) or( en_terminal_name='KY
Louisville - Valero - 3270' and supplier_name='Valero' and Account_Type='UNB Spot' ) or(
en_terminal_name='MO Brookline - Mag - 3718' and supplier_name='Valero' and
Account_Type='UNB Spot' ) or( en_terminal_name='TN Knoxville - Cummins - 2214' and
supplier_name='Valero' and Account_Type='UNB Spot' ) or( en_terminal_name='TN Knoxville - KM
- 2215' and supplier_name='Valero' and Account_Type='UNB Spot' ) or( en_terminal_name='AL
Birmingham - Citgo - 2302' and supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF
GAINESVILLE--224531' ) or( en_terminal_name='AL Birmingham - Citgo - 2302' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE-222307' ) or(
en_terminal_name='AL Birmingham - Citgo - 2302' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO/MYSTIK-376859' ) or( en_terminal_name='AL Birmingham - KM -
2307' and supplier_name='Valero' and Account_Type='UNB Spot' ) or( en_terminal_name='AR Fort
Smith - Mag - 2453' and supplier_name='Valero' and Account_Type='UNB Spot' ) or(
en_terminal_name='TX El Paso - Nustar - 2750' and supplier_name='Valero' and
Account_Type='UNB Spot' ) or( en_terminal_name='TX Grapevine - Nustar - 2680' and
supplier_name='Valero' and Account_Type='UNB Spot' ) or( en_terminal_name='TX San Antonio -
Nustar - 2738' and supplier_name='Valero' and Account_Type='UNB Spot' ) or(
en_terminal_name='TX San Antonio - Nustar - 2739' and supplier_name='Valero' and
Account_Type='UNB Spot' ) or( en_terminal_name='UT Salt Lake City - Chevron - 4203' and
supplier_name='Chevron' and Account_Type='MANSFIELD OIL COMPANY : CHV5246494' ) or(
en_terminal_name='WY Casper - Sinclair - 4058' and supplier_name='Exxon' and
Account_Type='MANSFIELD OIL COMPANY OF 106305 IW' ) or( en_terminal_name='AZ Phoenix -
Caljet - 4300' and supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S -
UNB Contract' ) or( en_terminal_name='CA Colton - SFPP - 4757' and supplier_name='Tesoro'
and Account_Type='UNBRANDED OPEN RACK' ) or( en_terminal_name='CA Colton - SFPP - 4757' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Contract' ) or(
en_terminal_name='0Unknown' and supplier_name='Shell' and Account_Type='MANSFIELD OIL
COMPANY OF 12313997' ) or( en_terminal_name='0Unknown' and supplier_name='Shell' and
Account_Type='MANSFIELD OIL COMPANY OF 12313167' ) or( en_terminal_name='0Unknown' and
supplier_name='Shell' and Account_Type='MANSFIELD OIL COMPANY OF 12323384' ) or(
en_terminal_name='0Unknown' and supplier_name='Shell' and Account_Type='MANSFIELD OIL
COMPANY OF 12324994' ) or( en_terminal_name='0Unknown' and supplier_name='Shell' and
Account_Type='Unknown')) OR (( en_terminal_name='CT Bridgeport - Sprague - 1256' and
supplier_name='Valero' and Account_Type='UNB Spot' and product_name='DSL - HSD/Heating Oil'
) or( en_terminal_name='CT Bridgeport - Sprague - 1256' and supplier_name='Valero' and
Account_Type='Unknown' and product_name='DSL - HSD/Heating Oil' ) or( en_terminal_name='CT
New Haven - Mag - 1274' and supplier_name='Valero' and Account_Type='UNB Spot' and
product_name='DSL - LSD/ULSD' ) or( en_terminal_name='CT New Haven - Mag - 1274' and
supplier_name='Valero' and Account_Type='UNB Spot' and product_name='Gas - Mid RFG' ) or(
en_terminal_name='CT New Haven - Mag - 1274' and supplier_name='Valero' and
Account_Type='UNB Spot' and product_name='Gas - Pre RFG' ) or( en_terminal_name='CT New
Haven - Mag - 1274' and supplier_name='Valero' and Account_Type='UNB Spot' and
product_name='Gas - Reg RFG' ) or( en_terminal_name='CT New Haven - Mag - 1274' and
supplier_name='Valero' and Account_Type='Unknown' and product_name='DSL - LSD/ULSD' ) or(
en_terminal_name='CT New Haven - Mag - 1274' and supplier_name='Valero' and
Account_Type='Unknown' and product_name='Gas - Mid RFG' ) or( en_terminal_name='CT New Haven
- Mag - 1274' and supplier_name='Valero' and Account_Type='Unknown' and product_name='Gas -
Pre RFG' ) or( en_terminal_name='CT New Haven - Mag - 1274' and supplier_name='Valero' and
Account_Type='Unknown' and product_name='Gas - Reg RFG' ) or( en_terminal_name='CT New Haven
- Motiva - 1254' and supplier_name='Shell' and Account_Type='MANSFIELD OIL COMPANY OF
12313997' and product_name='Commercial Gas Product-SH' ) or( en_terminal_name='CT New Haven
- Motiva - 1254' and supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S
- UNB Fwrd Cont' and product_name='Gas - All' ) or( en_terminal_name='CT New Haven - Motiva
- 1254' and supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB
Fwrd Cont' and product_name='Gas - All Pre/Mid' ) or( en_terminal_name='DE Delaware City -
Del City - 1600' and supplier_name='PBF' and Account_Type='MANSFIELD OIL COMPANY MERC' and
product_name='PBF 87' ) or( en_terminal_name='DE Delaware City - Del City - 1600' and
supplier_name='PBF' and Account_Type='MANSFIELD OIL COMPANY MERC' and product_name='PBF 93'
) or( en_terminal_name='DE Delaware City - Del City - 1600' and supplier_name='PBF' and
Account_Type='MANSFIELD OIL COMPANY MERC' and product_name='Unknown' ) or(
en_terminal_name='FL Cape Canaveral - TMG - 2138' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and product_name='Diesel Group' )
or( en_terminal_name='FL Cape Canaveral - TMG - 2138' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and product_name='GASOLINE GROUP' )
or( en_terminal_name='FL Dania Beach - Vecenergy - 2679' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='DSL -
LSD/ULSD' ) or( en_terminal_name='FL Dania Beach - Vecenergy - 2679' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Gas - All' ) or( en_terminal_name='FL Dania Beach - Vecenergy - 2679' and
supplier_name='Valero' and Account_Type='UNB Forward Contract' and product_name='DSL - Jet
Fuel' ) or( en_terminal_name='FL Dania Beach - Vecenergy - 2679' and supplier_name='Valero'
and Account_Type='UNB Spot' and product_name='DSL - Jet Fuel' ) or( en_terminal_name='FL
Dania Beach - Vecenergy - 2679' and supplier_name='Valero' and Account_Type='UNB Spot' and
product_name='DSL - LSD/ULSD' ) or( en_terminal_name='FL Dania Beach - Vecenergy - 2679' and
supplier_name='Valero' and Account_Type='UNB Spot' and product_name='Gas - Mid Conv' ) or(
en_terminal_name='FL Dania Beach - Vecenergy - 2679' and supplier_name='Valero' and
Account_Type='UNB Spot' and product_name='Gas - Pre Conv' ) or( en_terminal_name='FL Dania
Beach - Vecenergy - 2679' and supplier_name='Valero' and Account_Type='UNB Spot' and
product_name='Gas - Reg Conv' ) or( en_terminal_name='FL Dania Beach - Vecenergy - 2679' and
supplier_name='Valero' and Account_Type='UNB Spot' and product_name='Port Ev Rec Gas' ) or(
en_terminal_name='FL Fort Lauderdale - Chevron - 2153' and supplier_name='Chevron' and
Account_Type='MANSFIELD OIL COMPANY : CHV5246494' and product_name='' ) or(
en_terminal_name='FL Fort Lauderdale - Chevron - 2153' and supplier_name='Chevron' and
Account_Type='MANSFIELD OIL COMPANY : CHV5246494' and product_name='DIESEL #2' ) or(
en_terminal_name='FL Fort Lauderdale - Citgo - 2157' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and product_name='MID' ) or(
en_terminal_name='FL Fort Lauderdale - Citgo - 2157' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and product_name='PRM' ) or(
en_terminal_name='FL Fort Lauderdale - Citgo - 2157' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and product_name='ULS' ) or(
en_terminal_name='FL Fort Lauderdale - Citgo - 2157' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and product_name='UNL' ) or(
en_terminal_name='FL Fort Lauderdale - TMG - 2165' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and product_name='87 CONV E-10
GROUP' ) or( en_terminal_name='FL Fort Lauderdale - TMG - 2165' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and product_name='93 CONV E-10
GROUP' ) or( en_terminal_name='FL Fort Lauderdale - TMG - 2165' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and product_name='Diesel Group' )
or( en_terminal_name='FL Fort Lauderdale - TMG - 2165' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and product_name='GASOLINE GROUP' )
or( en_terminal_name='FL Freeport - Murphy - 2115' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='87 HI RVP' ) or( en_terminal_name='FL Freeport -
Murphy - 2115' and supplier_name='Murphy' and Account_Type='Unbranded' and product_name='87
UNL CONV' ) or( en_terminal_name='FL Freeport - Murphy - 2115' and supplier_name='Murphy'
and Account_Type='Unbranded' and product_name='87 UNL E10' ) or( en_terminal_name='FL
Freeport - Murphy - 2115' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='89 HI RVP' ) or( en_terminal_name='FL Freeport - Murphy - 2115' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='89 MID CONV' ) or(
en_terminal_name='FL Freeport - Murphy - 2115' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='89 MID E10' ) or( en_terminal_name='FL Freeport -
Murphy - 2115' and supplier_name='Murphy' and Account_Type='Unbranded' and product_name='92
PREMIUM E10' ) or( en_terminal_name='FL Freeport - Murphy - 2115' and supplier_name='Murphy'
and Account_Type='Unbranded' and product_name='93 PREMIUM CONV' ) or( en_terminal_name='FL
Freeport - Murphy - 2115' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='93 PREMIUM E10' ) or( en_terminal_name='FL Freeport - Murphy - 2115' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='C44' ) or(
en_terminal_name='FL Freeport - Murphy - 2115' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='CA8' ) or( en_terminal_name='FL Freeport - Murphy
- 2115' and supplier_name='Murphy' and Account_Type='Unbranded' and product_name='OXY 87 E10
HI RVP' ) or( en_terminal_name='FL Freeport - Murphy - 2115' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='OXY 92 E10 HI RVP' ) or( en_terminal_name='FL
Freeport - Murphy - 2115' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='OXY 93 E10 HI RVP' ) or( en_terminal_name='FL Freeport - Murphy - 2115' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='ULSD' ) or(
en_terminal_name='FL Freeport - Murphy - 2115' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='Unknown' ) or( en_terminal_name='FL Jacksonville
- Buckeye - 2102' and supplier_name='Shell' and Account_Type='MANSFIELD OIL COMPANY OF
12313997' and product_name='GENERIC ULSD-SH' ) or( en_terminal_name='FL Jacksonville -
Buckeye - 2102' and supplier_name='BP' and Account_Type='Channel: 50 - Commercial' and
product_name='DIESEL' ) or( en_terminal_name='FL Jacksonville - Center Pt - 2681' and
supplier_name='Valero' and Account_Type='UNB Spot' and product_name='Gas - Mid Conv' ) or(
en_terminal_name='FL Jacksonville - Center Pt - 2681' and supplier_name='Valero' and
Account_Type='UNB Spot' and product_name='Gas - Pre Conv' ) or( en_terminal_name='FL
Jacksonville - Center Pt - 2681' and supplier_name='Valero' and Account_Type='UNB Spot' and
product_name='Gas - Reg Conv' ) or( en_terminal_name='FL Jacksonville - MPC - 2106' and
supplier_name='Valero' and Account_Type='UNB Spot' and product_name='DSL - LSD/ULSD' ) or(
en_terminal_name='FL Orlando - CFPL - 2129' and supplier_name='Chevron' and
Account_Type='MANSFIELD OIL COMPANY : CHV5246494' and product_name='' ) or(
en_terminal_name='FL Orlando - CFPL - 2129' and supplier_name='Chevron' and
Account_Type='MANSFIELD OIL COMPANY : CHV5246494' and product_name='DIESEL #2' ) or(
en_terminal_name='FL Orlando - CFPL - 2129' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and product_name='MID' ) or(
en_terminal_name='FL Orlando - CFPL - 2129' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and product_name='PRM' ) or(
en_terminal_name='FL Orlando - CFPL - 2129' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and product_name='UNL' ) or(
en_terminal_name='FL Orlando - CFPL - 2129' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and product_name='Diesel Group' )
or( en_terminal_name='FL Orlando - CFPL - 2129' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and product_name='GASOLINE GROUP' )
or( en_terminal_name='FL Orlando - CFPL - 2129' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='DSL -
LSD/ULSD' ) or( en_terminal_name='FL Orlando - CFPL - 2129' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas - All' )
or( en_terminal_name='FL Orlando - CFPL - 2129' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas - Reg
Conv' ) or( en_terminal_name='FL Orlando - CFPL - 2129' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Unknown' )
or( en_terminal_name='FL Panama City - Chevron - 2116' and supplier_name='Chevron' and
Account_Type='MANSFIELD OIL COMPANY : CHV5246494' and product_name='' ) or(
en_terminal_name='FL Panama City - Chevron - 2116' and supplier_name='Chevron' and
Account_Type='MANSFIELD OIL COMPANY : CHV5246494' and product_name='DIESEL #2' ) or(
en_terminal_name='FL Panama City - Chevron - 2116' and supplier_name='Chevron' and
Account_Type='MANSFIELD OIL COMPANY : CHV5246494' and product_name='Unknown' ) or(
en_terminal_name='FL Panama City - Chevron - 2116' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='93 PREMIUM' ) or( en_terminal_name='FL Panama
City - Chevron - 2116' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='PREMIUM' ) or( en_terminal_name='FL Panama City - Chevron - 2116' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='REGULAR' ) or(
en_terminal_name='FL Panama City - Chevron - 2116' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='ULSD' ) or( en_terminal_name='FL Panama City -
Chevron - 2116' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='Unknown' ) or( en_terminal_name='FL Riviera Beach - Vecenergy - 2678' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='DSL - LSD/ULSD' ) or( en_terminal_name='FL Riviera Beach - Vecenergy - 2678'
and supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont'
and product_name='Gas - All' ) or( en_terminal_name='FL Riviera Beach - Vecenergy - 2678'
and supplier_name='Valero' and Account_Type='UNB Spot' and product_name='DSL - LSD/ULSD' )
or( en_terminal_name='FL Tampa - Citgo - 2133' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and product_name='MID' ) or(
en_terminal_name='FL Tampa - Citgo - 2133' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and product_name='PRM' ) or(
en_terminal_name='FL Tampa - Citgo - 2133' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and product_name='ULS' ) or(
en_terminal_name='FL Tampa - Citgo - 2133' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and product_name='UNL' ) or(
en_terminal_name='FL Tampa - Citgo - 2133' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO-FORWARD PRICE O-342837' and product_name='ULS' ) or(
en_terminal_name='FL Tampa - KM - 2123' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='DSL -
LSD/ULSD' ) or( en_terminal_name='FL Tampa - KM - 2123' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas - All' )
or( en_terminal_name='FL Tampa - KM - 2123' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas - All
Pre/Mid' ) or( en_terminal_name='FL Tampa - TMG - 2101' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and product_name='Diesel Group' )
or( en_terminal_name='FL Tampa - TMG - 2101' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and product_name='GASOLINE GROUP' )
or( en_terminal_name='GA Albany - KM - 2500' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='DSL -
LSD/ULSD' ) or( en_terminal_name='GA Albany - KM - 2500' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas - All' )
or( en_terminal_name='GA Albany - Mag - 2501' and supplier_name='P66' and Account_Type='UB
RACK' and product_name='C' ) or( en_terminal_name='GA Albany - Mag - 2501' and
supplier_name='P66' and Account_Type='UB RACK' and product_name='D' ) or(
en_terminal_name='GA Albany - Mag - 2501' and supplier_name='P66' and Account_Type='UB RACK'
and product_name='DISTILLATES' ) or( en_terminal_name='GA Albany - Mag - 2501' and
supplier_name='P66' and Account_Type='UB RACK' and product_name='P' ) or(
en_terminal_name='GA Albany - Mag - 2501' and supplier_name='Valero' and Account_Type='UNB
Spot' and product_name='DSL - LSD/ULSD' ) or( en_terminal_name='GA Albany - Mag - 2501' and
supplier_name='Valero' and Account_Type='UNB Spot' and product_name='Gas - Pre Conv' ) or(
en_terminal_name='GA Albany - Mag - 2501' and supplier_name='Valero' and Account_Type='UNB
Spot' and product_name='Gas - Reg Conv' ) or( en_terminal_name='GA Athens - KM - 2506' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='ULS' ) or( en_terminal_name='GA Athens - KM - 2506' and supplier_name='Murphy'
and Account_Type='Unbranded' and product_name='89 MID' ) or( en_terminal_name='GA Athens -
KM - 2506' and supplier_name='Murphy' and Account_Type='Unbranded' and product_name='93
PREMIUM' ) or( en_terminal_name='GA Athens - KM - 2506' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='LSD' ) or( en_terminal_name='GA Athens - KM -
2506' and supplier_name='Murphy' and Account_Type='Unbranded' and product_name='MIDGRADE' )
or( en_terminal_name='GA Athens - KM - 2506' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='PREMIUM' ) or( en_terminal_name='GA Athens - KM -
2506' and supplier_name='Murphy' and Account_Type='Unbranded' and product_name='REGULAR' )
or( en_terminal_name='GA Athens - KM - 2506' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='ULSD' ) or( en_terminal_name='GA Athens - KM -
2506' and supplier_name='Murphy' and Account_Type='Unbranded' and product_name='Unknown' )
or( en_terminal_name='GA Athens - KM - 2506' and supplier_name='P66' and Account_Type='UB
RACK' and product_name='C' ) or( en_terminal_name='GA Athens - KM - 2506' and
supplier_name='P66' and Account_Type='UB RACK' and product_name='D' ) or(
en_terminal_name='GA Athens - KM - 2506' and supplier_name='P66' and Account_Type='UB RACK'
and product_name='DISTILLATES' ) or( en_terminal_name='GA Athens - KM - 2506' and
supplier_name='P66' and Account_Type='UB RACK' and product_name='P' ) or(
en_terminal_name='GA Athens - KM - 2506' and supplier_name='P66' and Account_Type='UB RACK'
and product_name='Unknown' ) or( en_terminal_name='GA Athens - KM - 2506' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='DSL - LSD/ULSD' ) or( en_terminal_name='GA Athens - KM - 2506' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Gas - All' ) or( en_terminal_name='GA Athens - KM - 2506' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Unknown' ) or( en_terminal_name='GA Atlanta - Perimeter - 2519' and
supplier_name='Shell' and Account_Type='MANSFIELD OIL COMPANY OF 12313997' and
product_name='GENERIC ULSD-SH' ) or( en_terminal_name='GA Atlanta - Perimeter - 2519' and
supplier_name='Valero' and Account_Type='UNB Spot' and product_name='DSL - LSD/ULSD' ) or(
en_terminal_name='GA Atlanta - Perimeter - 2519' and supplier_name='Valero' and
Account_Type='UNB Spot' and product_name='Gas - Flex Fuel' ) or( en_terminal_name='GA
Atlanta - Perimeter - 2519' and supplier_name='Valero' and Account_Type='UNB Spot' and
product_name='Gas - Pre Conv' ) or( en_terminal_name='GA Atlanta - Perimeter - 2519' and
supplier_name='Valero' and Account_Type='UNB Spot' and product_name='Gas - Reg Conv' ) or(
en_terminal_name='GA Atlanta - Perimeter - 2519' and supplier_name='Valero' and
Account_Type='UNB Spot' and product_name='Perimeter Atlanta REC GAS' ) or(
en_terminal_name='GA Bainbridge - Motiva - 2514' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas - All' )
or( en_terminal_name='GA Bainbridge - Motiva - 2514' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Unknown' )
or( en_terminal_name='GA Bainbridge - Motiva - 2514' and supplier_name='Valero' and
Account_Type='UNB Spot' and product_name='DSL - HSD/Heating Oil' ) or( en_terminal_name='GA
Bainbridge - Motiva - 2514' and supplier_name='Valero' and Account_Type='UNB Spot' and
product_name='DSL - LSD/ULSD' ) or( en_terminal_name='GA Bainbridge - Motiva - 2514' and
supplier_name='Valero' and Account_Type='UNB Spot' and product_name='Gas - Mid Conv' ) or(
en_terminal_name='GA Bainbridge - Motiva - 2514' and supplier_name='Valero' and
Account_Type='UNB Spot' and product_name='Gas - Pre Conv' ) or( en_terminal_name='GA
Bainbridge - Motiva - 2514' and supplier_name='Valero' and Account_Type='UNB Spot' and
product_name='Gas - Reg Conv' ) or( en_terminal_name='GA Bainbridge - TMG - 2515' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='ULS' ) or( en_terminal_name='GA Bainbridge - TMG - 2515' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='89 MID' ) or(
en_terminal_name='GA Bainbridge - TMG - 2515' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='93 PREMIUM' ) or( en_terminal_name='GA Bainbridge
- TMG - 2515' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='MIDGRADE' ) or( en_terminal_name='GA Bainbridge - TMG - 2515' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='PREMIUM' ) or(
en_terminal_name='GA Bainbridge - TMG - 2515' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='REGULAR' ) or( en_terminal_name='GA Bainbridge -
TMG - 2515' and supplier_name='Murphy' and Account_Type='Unbranded' and product_name='ULSD'
) or( en_terminal_name='GA Bainbridge - TMG - 2515' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='Unknown' ) or( en_terminal_name='GA Columbus - KM
- 2520' and supplier_name='Valero' and Account_Type='UNB Spot' and product_name='DSL -
LSD/ULSD' ) or( en_terminal_name='GA Columbus - KM - 2520' and supplier_name='Valero' and
Account_Type='UNB Spot' and product_name='Gas - Mid Conv' ) or( en_terminal_name='GA
Columbus - KM - 2520' and supplier_name='Valero' and Account_Type='UNB Spot' and
product_name='Gas - Pre Conv' ) or( en_terminal_name='GA Columbus - KM - 2520' and
supplier_name='Valero' and Account_Type='UNB Spot' and product_name='Gas - Reg Conv' ) or(
en_terminal_name='GA Doraville - Citgo - 2529' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO-FORWARD PRICE O-342837' and product_name='ULS' ) or(
en_terminal_name='GA Doraville - Citgo - 2529' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas - All' )
or( en_terminal_name='GA Doraville - Citgo - 2529' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Unknown' )
or( en_terminal_name='GA Doraville - Mag - 2535' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='89 MID' ) or( en_terminal_name='GA Doraville -
Mag - 2535' and supplier_name='Murphy' and Account_Type='Unbranded' and product_name='93
PREMIUM' ) or( en_terminal_name='GA Doraville - Mag - 2535' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='MIDGRADE' ) or( en_terminal_name='GA Doraville -
Mag - 2535' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='PREMIUM' ) or( en_terminal_name='GA Doraville - Mag - 2535' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='REGULAR' ) or(
en_terminal_name='GA Doraville - Mag - 2535' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='ULSD' ) or( en_terminal_name='GA Doraville - Mag
- 2535' and supplier_name='P66' and Account_Type='UB RACK' and product_name='C' ) or(
en_terminal_name='GA Doraville - Mag - 2535' and supplier_name='P66' and Account_Type='UB
RACK' and product_name='D' ) or( en_terminal_name='GA Doraville - Mag - 2535' and
supplier_name='P66' and Account_Type='UB RACK' and product_name='DISTILLATES' ) or(
en_terminal_name='GA Doraville - Mag - 2535' and supplier_name='P66' and Account_Type='UB
RACK' and product_name='P' ) or( en_terminal_name='GA Doraville - Motiva - 2510' and
supplier_name='Valero' and Account_Type='UNB Spot' and product_name='DSL - HSD/Heating Oil'
) or( en_terminal_name='GA Doraville - Motiva - 2510' and supplier_name='Valero' and
Account_Type='UNB Spot' and product_name='DSL - LSD/ULSD' ) or( en_terminal_name='GA
Doraville - Motiva - 2510' and supplier_name='Valero' and Account_Type='UNB Spot' and
product_name='Gas - Mid Conv' ) or( en_terminal_name='GA Doraville - Motiva - 2510' and
supplier_name='Valero' and Account_Type='UNB Spot' and product_name='Gas - Mid RFG' ) or(
en_terminal_name='GA Doraville - Motiva - 2510' and supplier_name='Valero' and
Account_Type='UNB Spot' and product_name='Gas - Pre Conv' ) or( en_terminal_name='GA
Doraville - Motiva - 2510' and supplier_name='Valero' and Account_Type='UNB Spot' and
product_name='Gas - Pre RFG' ) or( en_terminal_name='GA Doraville - Motiva - 2510' and
supplier_name='Valero' and Account_Type='UNB Spot' and product_name='Gas - Reg Conv' ) or(
en_terminal_name='GA Doraville - Motiva - 2510' and supplier_name='Valero' and
Account_Type='UNB Spot' and product_name='Gas - Reg RFG' ) or( en_terminal_name='GA Lookout
Mtn - TMG - 2556' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='87 UNL CONV' ) or( en_terminal_name='GA Lookout Mtn - TMG - 2556' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='89 MID' ) or(
en_terminal_name='GA Lookout Mtn - TMG - 2556' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='93 PREMIUM' ) or( en_terminal_name='GA Lookout
Mtn - TMG - 2556' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='B' ) or( en_terminal_name='GA Lookout Mtn - TMG - 2556' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='ETHANOL' ) or(
en_terminal_name='GA Lookout Mtn - TMG - 2556' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='LSD' ) or( en_terminal_name='GA Lookout Mtn - TMG
- 2556' and supplier_name='Murphy' and Account_Type='Unbranded' and product_name='MIDGRADE'
) or( en_terminal_name='GA Lookout Mtn - TMG - 2556' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='PREMIUM' ) or( en_terminal_name='GA Lookout Mtn -
TMG - 2556' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='REGULAR' ) or( en_terminal_name='GA Lookout Mtn - TMG - 2556' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='ULSD' ) or(
en_terminal_name='GA Lookout Mtn - TMG - 2556' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='Unknown' ) or( en_terminal_name='GA Macon - Mag -
2543' and supplier_name='P66' and Account_Type='UB RACK' and product_name='C' ) or(
en_terminal_name='GA Macon - Mag - 2543' and supplier_name='P66' and Account_Type='UB RACK'
and product_name='D' ) or( en_terminal_name='GA Macon - Mag - 2543' and supplier_name='P66'
and Account_Type='UB RACK' and product_name='DISTILLATES' ) or( en_terminal_name='GA Macon -
Mag - 2543' and supplier_name='P66' and Account_Type='UB RACK' and product_name='P' ) or(
en_terminal_name='GA Macon - Mag - 2543' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='DSL -
LSD/ULSD' ) or( en_terminal_name='GA Macon - Mag - 2543' and supplier_name='Valero' and
Account_Type='UNB Spot' and product_name='DSL - LSD/ULSD' ) or( en_terminal_name='GA Macon -
Mag - 2543' and supplier_name='Valero' and Account_Type='UNB Spot' and product_name='Gas -
Pre Conv' ) or( en_terminal_name='GA Macon - Mag - 2543' and supplier_name='Valero' and
Account_Type='UNB Spot' and product_name='Gas - Reg Conv' ) or( en_terminal_name='GA Macon -
Vecenergy - 2538' and supplier_name='Chevron' and Account_Type='MANSFIELD OIL COMPANY :
CHV5246494' and product_name='DIESEL #2' ) or( en_terminal_name='GA Macon - Vecenergy -
2538' and supplier_name='Gulf' and Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT'
and product_name='Diesel Group' ) or( en_terminal_name='GA Macon - Vecenergy - 2538' and
supplier_name='Gulf' and Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and
product_name='GASOLINE GROUP' ) or( en_terminal_name='GA Macon - Vecenergy - 2538' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='DSL - LSD/ULSD' ) or( en_terminal_name='MA Braintree - Citgo - 1155' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='HOM' ) or( en_terminal_name='MA Braintree - Citgo - 1155' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='ULS' ) or( en_terminal_name='MA Braintree - Citgo - 1155' and
supplier_name='Gulf' and Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and
product_name='Diesel Group' ) or( en_terminal_name='MA Braintree - Citgo - 1155' and
supplier_name='Gulf' and Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and
product_name='GASOLINE GROUP' ) or( en_terminal_name='MD Baltimore - Citgo - 1562' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Gas - All' ) or( en_terminal_name='MD Baltimore - Citgo - 1562' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Unknown' ) or( en_terminal_name='MD Baltimore - Citgo - 1562' and
supplier_name='Valero' and Account_Type='UNB Spot' and product_name='DSL - LSD/ULSD' ) or(
en_terminal_name='MD Baltimore - Citgo - 1562' and supplier_name='Valero' and
Account_Type='UNB Spot' and product_name='Gas - Mid RFG' ) or( en_terminal_name='MD
Baltimore - Citgo - 1562' and supplier_name='Valero' and Account_Type='UNB Spot' and
product_name='Gas - Pre RFG' ) or( en_terminal_name='MD Baltimore - Citgo - 1562' and
supplier_name='Valero' and Account_Type='UNB Spot' and product_name='Gas - Reg RFG' ) or(
en_terminal_name='MD Baltimore - Motiva - 1561' and supplier_name='Shell' and
Account_Type='MANSFIELD OIL COMPANY OF 12313997' and product_name='Commercial Gas Product-
SH' ) or( en_terminal_name='MD Baltimore - Motiva - 1561' and supplier_name='Shell' and
Account_Type='MANSFIELD OIL COMPANY OF 12313997' and product_name='GENERIC ULSD-SH' ) or(
en_terminal_name='MD Baltimore - Sunoco - 1552' and supplier_name='P66' and Account_Type='UB
RACK' and product_name='DISTILLATES' ) or( en_terminal_name='MD Baltimore - Sunoco - 1552'
and supplier_name='P66' and Account_Type='UB RACK' and product_name='O' ) or(
en_terminal_name='MD Baltimore - Sunoco - 1552' and supplier_name='P66' and Account_Type='UB
RACK' and product_name='Q' ) or( en_terminal_name='MD Baltimore - Sunoco - 1552' and
supplier_name='P66' and Account_Type='UB RACK' and product_name='R' ) or(
en_terminal_name='MD Baltimore - Buckeye - 1550' and supplier_name='BP' and
Account_Type='Channel: 50 - Commercial' and product_name='MID-GRADE' ) or(
en_terminal_name='MD Baltimore - Buckeye - 1550' and supplier_name='BP' and
Account_Type='Channel: 50 - Commercial' and product_name='PREMIUM' ) or(
en_terminal_name='MD Baltimore - Buckeye - 1550' and supplier_name='BP' and
Account_Type='Channel: 50 - Commercial' and product_name='REGULAR' ) or(
en_terminal_name='NC Charlotte - Citgo - 2001' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and product_name='MID' ) or(
en_terminal_name='NC Charlotte - Citgo - 2001' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and product_name='PRM' ) or(
en_terminal_name='NC Charlotte - Citgo - 2001' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and product_name='ULS' ) or(
en_terminal_name='NC Charlotte - Citgo - 2001' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and product_name='UNL' ) or(
en_terminal_name='NC Charlotte - Citgo - 2001' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO-FORWARD PRICE O-342837' and product_name='ULS' ) or(
en_terminal_name='NC Charlotte - KM - 2000' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='DSL -
LSD/ULSD' ) or( en_terminal_name='NC Charlotte - KM - 2000' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas - All' )
or( en_terminal_name='NC Charlotte - KM - 2000' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas - All
Pre/Mid' ) or( en_terminal_name='NC Charlotte - KM - 2004' and supplier_name='Valero' and
Account_Type='UNB Spot' and product_name='Unknown' ) or( en_terminal_name='NC Charlotte - KM
- 2026' and supplier_name='Valero' and Account_Type='UNB Spot' and product_name='Unknown' )
or( en_terminal_name='NC Charlotte - Mag - 2006' and supplier_name='P66' and
Account_Type='UB RACK' and product_name='D' ) or( en_terminal_name='NC Charlotte - Mag -
2006' and supplier_name='P66' and Account_Type='UB RACK' and product_name='DISTILLATES' )
or( en_terminal_name='NC Charlotte - Mag - 2006' and supplier_name='P66' and
Account_Type='UB RACK' and product_name='P' ) or( en_terminal_name='NC Charlotte - Mag -
2024' and supplier_name='P66' and Account_Type='UB RACK' and product_name='D' ) or(
en_terminal_name='NC Charlotte - Mag - 2024' and supplier_name='P66' and Account_Type='UB
RACK' and product_name='DISTILLATES' ) or( en_terminal_name='NC Charlotte - Mag - 2024' and
supplier_name='P66' and Account_Type='UB RACK' and product_name='P' ) or(
en_terminal_name='NC Charlotte - Motiva - 2005' and supplier_name='Shell' and
Account_Type='MANSFIELD OIL COMPANY OF 12313997' and product_name='GENERIC ULSD-SH' ) or(
en_terminal_name='NC Charlotte - Motiva - 2007' and supplier_name='Shell' and
Account_Type='MANSFIELD OIL COMPANY OF 12313997' and product_name='Commercial Gas Product-
SH' ) or( en_terminal_name='NC Charlotte - Motiva - 2007' and supplier_name='Shell' and
Account_Type='MANSFIELD OIL COMPANY OF 12313997' and product_name='GENERIC ULSD-SH' ) or(
en_terminal_name='NC Charlotte - MPC - 2008' and supplier_name='BP' and Account_Type='18 -
Unbranded Jobbers' and product_name='DIESEL' ) or( en_terminal_name='NC Charlotte - MPC -
2008' and supplier_name='BP' and Account_Type='18 - Unbranded Jobbers' and
product_name='MID-GRADE 9' ) or( en_terminal_name='NC Charlotte - MPC - 2008' and
supplier_name='BP' and Account_Type='18 - Unbranded Jobbers' and product_name='PREMIUM 9' )
or( en_terminal_name='NC Fayetteville - Motiva - 2009' and supplier_name='Shell' and
Account_Type='MANSFIELD OIL COMPANY OF 12313997' and product_name='Commercial Gas Product-
SH' ) or( en_terminal_name='NC Greensboro - KM - 2014' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL GAIN - UNBR FXD GBORO 7.8' and product_name='GBORO - 7.8 PREM' )
or( en_terminal_name='NC Greensboro - KM - 2014' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL GAIN - UNBR FXD GBORO 7.8' and product_name='GBORO - 7.8 REG' )
or( en_terminal_name='NC Greensboro - KM - 2014' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL GAIN - UNBR FXD GBORO 9.0' and product_name='GBORO - 9.0 PREM' )
or( en_terminal_name='NC Greensboro - KM - 2014' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL GAIN - UNBR FXD GBORO 9.0' and product_name='GBORO - 9.0 REG' )
or( en_terminal_name='NC Greensboro - KM - 2014' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas - All' )
or( en_terminal_name='NC Greensboro - KM - 2015' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas - All' )
or( en_terminal_name='NC Greensboro - Mag - 2011' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and product_name='PRM' ) or(
en_terminal_name='NC Greensboro - Mag - 2011' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and product_name='ULS' ) or(
en_terminal_name='NC Greensboro - Mag - 2011' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and product_name='UNL' ) or(
en_terminal_name='NC Greensboro - Mag - 2011' and supplier_name='P66' and Account_Type='UB
RACK' and product_name='D' ) or( en_terminal_name='NC Greensboro - Mag - 2011' and
supplier_name='P66' and Account_Type='UB RACK' and product_name='DISTILLATES' ) or(
en_terminal_name='NC Greensboro - Mag - 2011' and supplier_name='P66' and Account_Type='UB
RACK' and product_name='P' ) or( en_terminal_name='NC Greensboro - Mag - 2020' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='ULS' ) or( en_terminal_name='NC Greensboro - Motiva - 2021' and
supplier_name='Shell' and Account_Type='MANSFIELD OIL COMPANY OF 12313997' and
product_name='Commercial Gas Product-SH' ) or( en_terminal_name='NC Greensboro - Motiva -
2021' and supplier_name='Shell' and Account_Type='MANSFIELD OIL COMPANY OF 12313997' and
product_name='GENERIC ULSD-SH' ) or( en_terminal_name='NC Greensboro - Motiva - 2021' and
supplier_name='Shell' and Account_Type='MANSFIELD OIL COMPANY OF 12323384' and
product_name='GENERIC ULSD-SH' ) or( en_terminal_name='NC Selma - Citgo - 2030' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='MID' ) or( en_terminal_name='NC Selma - Citgo - 2030' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='PRM' ) or( en_terminal_name='NC Selma - Citgo - 2030' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='ULS' ) or( en_terminal_name='NC Selma - Citgo - 2030' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='UNL' ) or( en_terminal_name='NC Selma - KM - 2013' and supplier_name='Valero'
and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas -
All' ) or( en_terminal_name='NC Selma - KM - 2018' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='DSL -
LSD/ULSD' ) or( en_terminal_name='NC Selma - KM - 2018' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas - All' )
or( en_terminal_name='NC Selma - KM - 2018' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas - Pre
Conv' ) or( en_terminal_name='NC Selma - KM - 2018' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas - Reg
Conv' ) or( en_terminal_name='NC Selma - KM - 2034' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and product_name='Diesel Group' )
or( en_terminal_name='NC Selma - KM - 2034' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and product_name='GASOLINE GROUP' )
or( en_terminal_name='NC Selma - Mag - 2036' and supplier_name='P66' and Account_Type='UB
RACK' and product_name='C' ) or( en_terminal_name='NC Selma - Mag - 2036' and
supplier_name='P66' and Account_Type='UB RACK' and product_name='D' ) or(
en_terminal_name='NC Selma - Mag - 2036' and supplier_name='P66' and Account_Type='UB RACK'
and product_name='DISTILLATES' ) or( en_terminal_name='NC Selma - Mag - 2036' and
supplier_name='P66' and Account_Type='UB RACK' and product_name='P' ) or(
en_terminal_name='NJ Linden - Citgo - 1513' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas - All' )
or( en_terminal_name='NJ Linden - Citgo - 1513' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas - All
Pre/Mid' ) or( en_terminal_name='NJ Linden - Citgo - 1513' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Unknown' )
or( en_terminal_name='NJ Linden - Gulf - 1515' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and product_name='Diesel Group' )
or( en_terminal_name='NJ Linden - Gulf - 1515' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and product_name='GASOLINE GROUP' )
or( en_terminal_name='NJ Linden - Gulf - 1515' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and product_name='HEAT GROUP' ) or(
en_terminal_name='NJ Newark - Motiva - 1521' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='DSL -
LSD/ULSD' ) or( en_terminal_name='NJ Newark - Motiva - 1521' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas - All' )
or( en_terminal_name='NJ Newark - Motiva - 1521' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas - All
Pre/Mid' ) or( en_terminal_name='NJ Paulsboro - Nustar - 1526' and supplier_name='PBF' and
Account_Type='MANSFIELD OIL COMPANY MERC' and product_name='PBF 87' ) or(
en_terminal_name='NJ Paulsboro - Nustar - 1526' and supplier_name='PBF' and
Account_Type='MANSFIELD OIL COMPANY MERC' and product_name='PBF 93' ) or(
en_terminal_name='NJ Paulsboro - Nustar - 1526' and supplier_name='PBF' and
Account_Type='MANSFIELD OIL COMPANY MERC' and product_name='Unknown' ) or(
en_terminal_name='NJ Sewaren - Motiva - 1538' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas - All' )
or( en_terminal_name='NJ Sewaren - Motiva - 1538' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas - All
Pre/Mid' ) or( en_terminal_name='NY Lawrence - Motiva - 1312' and supplier_name='Shell' and
Account_Type='MANSFIELD OIL COMPANY OF 12313997' and product_name='GENERIC ULSD-SH' ) or(
en_terminal_name='NY New Windsor - Global - 1411' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and product_name='Diesel Group' )
or( en_terminal_name='NY New Windsor - Global - 1411' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and product_name='GASOLINE GROUP' )
or( en_terminal_name='NY New Windsor - Global - 1411' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='DSL -
LSD/ULSD' ) or( en_terminal_name='NY New Windsor - Global - 1411' and supplier_name='Valero'
and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas -
All' ) or( en_terminal_name='NY New Windsor - Global - 1411' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas - All
Pre/Mid' ) or( en_terminal_name='NY New Windsor - Global - 1411' and supplier_name='Valero'
and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Unknown'
) or( en_terminal_name='NY New Windsor - Global - 1413' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and product_name='Diesel Group' )
or( en_terminal_name='NY New Windsor - Global - 1413' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and product_name='GASOLINE GROUP' )
or( en_terminal_name='PA Coraopolis - PPC - 1780' and supplier_name='Shell' and
Account_Type='MANSFIELD OIL COMPANY OF 12313997' and product_name='GENERIC ULSD-SH' ) or(
en_terminal_name='PA Mechanicsburg - Gulf - 1725' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and product_name='Diesel Group' )
or( en_terminal_name='PA Mechanicsburg - Gulf - 1725' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and product_name='GASOLINE GROUP' )
or( en_terminal_name='PA Mechanicsburg - Gulf - 1725' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and product_name='HEAT GROUP' ) or(
en_terminal_name='PA Mechanicsburg - PPC - 1715' and supplier_name='Shell' and
Account_Type='MANSFIELD OIL COMPANY OF 12313997' and product_name='GENERIC ULSD-SH' ) or(
en_terminal_name='PA Mechanicsburg - PPC - 1715' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='DSL -
LSD/ULSD' ) or( en_terminal_name='PA Mechanicsburg - PPC - 1715' and supplier_name='Valero'
and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas -
All' ) or( en_terminal_name='PA Mechanicsburg - PPC - 1715' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas - All
Pre/Mid' ) or( en_terminal_name='PA Mechanicsburg - PPC - 1715' and supplier_name='Valero'
and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Unknown'
) or( en_terminal_name='PA Mechanicsburg - PPC - 1715' and supplier_name='Valero' and
Account_Type='UNB Spot' and product_name='DSL - HSD/Heating Oil' ) or( en_terminal_name='PA
Mechanicsburg - PPC - 1715' and supplier_name='Valero' and Account_Type='UNB Spot' and
product_name='DSL - LSD/ULSD' ) or( en_terminal_name='PA Mechanicsburg - PPC - 1715' and
supplier_name='Valero' and Account_Type='UNB Spot' and product_name='Gas - Flex Fuel' ) or(
en_terminal_name='PA Mechanicsburg - PPC - 1715' and supplier_name='Valero' and
Account_Type='UNB Spot' and product_name='Gas - Mid Conv' ) or( en_terminal_name='PA
Mechanicsburg - PPC - 1715' and supplier_name='Valero' and Account_Type='UNB Spot' and
product_name='Gas - Pre Conv' ) or( en_terminal_name='PA Mechanicsburg - PPC - 1715' and
supplier_name='Valero' and Account_Type='UNB Spot' and product_name='Gas - Reg Conv' ) or(
en_terminal_name='PA Mechanicsburg - Pyramid - 1715' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='DSL -
LSD/ULSD' ) or( en_terminal_name='PA Mechanicsburg - Pyramid - 1715' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Gas - All' ) or( en_terminal_name='PA Mechanicsburg - Pyramid - 1715' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Gas - All Pre/Mid' ) or( en_terminal_name='PA Mechanicsburg - Pyramid - 1715'
and supplier_name='Valero' and Account_Type='UNB Spot' and product_name='DSL - HSD/Heating
Oil' ) or( en_terminal_name='PA Mechanicsburg - Pyramid - 1715' and supplier_name='Valero'
and Account_Type='UNB Spot' and product_name='DSL - LSD/ULSD' ) or( en_terminal_name='PA
Mechanicsburg - Pyramid - 1715' and supplier_name='Valero' and Account_Type='UNB Spot' and
product_name='Gas - Flex Fuel' ) or( en_terminal_name='PA Mechanicsburg - Pyramid - 1715'
and supplier_name='Valero' and Account_Type='UNB Spot' and product_name='Gas - Mid Conv' )
or( en_terminal_name='PA Mechanicsburg - Pyramid - 1715' and supplier_name='Valero' and
Account_Type='UNB Spot' and product_name='Gas - Pre Conv' ) or( en_terminal_name='PA
Mechanicsburg - Pyramid - 1715' and supplier_name='Valero' and Account_Type='UNB Spot' and
product_name='Gas - Reg Conv' ) or( en_terminal_name='PA Middletown - PPC - 1716' and
supplier_name='Shell' and Account_Type='MANSFIELD OIL COMPANY OF 12313997' and
product_name='GENERIC ULSD-SH' ) or( en_terminal_name='PA Middletown - PPC - 1716' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='DSL - LSD/ULSD' ) or( en_terminal_name='PA Middletown - PPC - 1716' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Gas - All' ) or( en_terminal_name='PA Middletown - PPC - 1716' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Gas - All Pre/Mid' ) or( en_terminal_name='PA Middletown - PPC - 1716' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Unknown' ) or( en_terminal_name='PA Philadelphia - Plains - 1737' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Gas - All' ) or( en_terminal_name='PA Pittsburgh - Gulf - 1777' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='DSL - LSD/ULSD' ) or( en_terminal_name='PA Pittsburgh - Gulf - 1777' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Unknown' ) or( en_terminal_name='PA Pittsburgh - PPC - 1776' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='DSL - LSD/ULSD' ) or( en_terminal_name='PA Pittsburgh - PPC - 1776' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Gas - All' ) or( en_terminal_name='PA Pittsburgh - PPC - 1776' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Unknown' ) or( en_terminal_name='PA Pittsburgh - Pyramid - 1776' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='DSL - LSD/ULSD' ) or( en_terminal_name='PA Pittsburgh - Pyramid - 1776' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Gas - All' ) or( en_terminal_name='PA Pittston - PPC - 1707' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='DSL - LSD/ULSD' ) or( en_terminal_name='PA Pittston - PPC - 1707' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Unknown' ) or( en_terminal_name='PA Pittston - Pyramid - 1707' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='DSL - LSD/ULSD' ) or( en_terminal_name='SC Belton - Buckeye - 2051' and
supplier_name='Gulf' and Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and
product_name='Diesel Group' ) or( en_terminal_name='SC Belton - Buckeye - 2051' and
supplier_name='Gulf' and Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and
product_name='GASOLINE GROUP' ) or( en_terminal_name='SC North Augusta - KM - 2060' and
supplier_name='Shell' and Account_Type='MANSFIELD OIL COMPANY OF 12313997' and
product_name='Commercial Gas Product-SH' ) or( en_terminal_name='SC North Augusta - KM -
2060' and supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB
Contract' and product_name='Gas - Pre Conv' ) or( en_terminal_name='SC North Augusta - KM -
2060' and supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB
Contract' and product_name='Gas - Pre RFG' ) or( en_terminal_name='SC North Augusta - KM -
2060' and supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB
Contract' and product_name='Gas - Reg Conv' ) or( en_terminal_name='SC North Augusta - KM -
2060' and supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd
Cont' and product_name='DSL - LSD/ULSD' ) or( en_terminal_name='SC North Augusta - KM -
2060' and supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd
Cont' and product_name='Gas - All' ) or( en_terminal_name='SC North Augusta - KM - 2060' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Gas - All Pre/Mid' ) or( en_terminal_name='SC North Augusta - KM - 2062' and
supplier_name='Chevron' and Account_Type='MANSFIELD OIL COMPANY : CHV5246494' and
product_name='DIESEL #2' ) or( en_terminal_name='SC North Augusta - KM - 2062' and
supplier_name='Chevron' and Account_Type='MANSFIELD OIL COMPANY : CHV5246494' and
product_name='GASOLINE' ) or( en_terminal_name='SC North Augusta - KM - 2062' and
supplier_name='P66' and Account_Type='UB RACK' and product_name='D' ) or(
en_terminal_name='SC North Augusta - KM - 2062' and supplier_name='P66' and Account_Type='UB
RACK' and product_name='DISTILLATES' ) or( en_terminal_name='SC North Augusta - KM - 2062'
and supplier_name='P66' and Account_Type='UB RACK' and product_name='P' ) or(
en_terminal_name='SC North Augusta - Mag - 2059' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and product_name='ULS' ) or(
en_terminal_name='SC North Augusta - Mag - 2059' and supplier_name='P66' and
Account_Type='UB RACK' and product_name='C' ) or( en_terminal_name='SC North Augusta - Mag -
2059' and supplier_name='P66' and Account_Type='UB RACK' and product_name='D' ) or(
en_terminal_name='SC North Augusta - Mag - 2059' and supplier_name='P66' and
Account_Type='UB RACK' and product_name='DISTILLATES' ) or( en_terminal_name='SC North
Augusta - Mag - 2059' and supplier_name='P66' and Account_Type='UB RACK' and
product_name='P' ) or( en_terminal_name='SC North Augusta - Mag - 2063' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='MID' ) or( en_terminal_name='SC North Augusta - Mag - 2063' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='PRM' ) or( en_terminal_name='SC North Augusta - Mag - 2063' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='ULS' ) or( en_terminal_name='SC North Augusta - Mag - 2063' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='UNL' ) or( en_terminal_name='SC North Augusta - Mag - 2063' and
supplier_name='P66' and Account_Type='UB RACK' and product_name='C' ) or(
en_terminal_name='SC North Augusta - Mag - 2063' and supplier_name='P66' and
Account_Type='UB RACK' and product_name='D' ) or( en_terminal_name='SC North Augusta - Mag -
2063' and supplier_name='P66' and Account_Type='UB RACK' and product_name='DISTILLATES' )
or( en_terminal_name='SC North Augusta - Mag - 2063' and supplier_name='P66' and
Account_Type='UB RACK' and product_name='P' ) or( en_terminal_name='SC North Charleston -
Buckeye - 2064' and supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF
GAINESVILLE--224531' and product_name='MID' ) or( en_terminal_name='SC North Charleston -
Buckeye - 2064' and supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF
GAINESVILLE--224531' and product_name='PRM' ) or( en_terminal_name='SC North Charleston -
Buckeye - 2064' and supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF
GAINESVILLE--224531' and product_name='ULS' ) or( en_terminal_name='SC North Charleston -
Buckeye - 2064' and supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF
GAINESVILLE--224531' and product_name='UNL' ) or( en_terminal_name='SC Spartanburg - Mag -
2076' and supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd
Cont' and product_name='Gas - All' ) or( en_terminal_name='SC Spartanburg - Buckeye - 2052'
and supplier_name='Murphy' and Account_Type='Unbranded' and product_name='KEROSENE' ) or(
en_terminal_name='SC Spartanburg - Buckeye - 2052' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='L' ) or( en_terminal_name='SC Spartanburg -
Buckeye - 2052' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='ULSD' ) or( en_terminal_name='SC Spartanburg - Buckeye - 2052' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='Unknown' ) or(
en_terminal_name='SC Spartanburg - KM - 2074' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas - All' )
or( en_terminal_name='SC Spartanburg - Mag - 2068' and supplier_name='P66' and
Account_Type='UB RACK' and product_name='C' ) or( en_terminal_name='SC Spartanburg - Mag -
2068' and supplier_name='P66' and Account_Type='UB RACK' and product_name='D' ) or(
en_terminal_name='SC Spartanburg - Mag - 2068' and supplier_name='P66' and Account_Type='UB
RACK' and product_name='DISTILLATES' ) or( en_terminal_name='SC Spartanburg - Mag - 2068'
and supplier_name='P66' and Account_Type='UB RACK' and product_name='P' ) or(
en_terminal_name='SC Spartanburg - Mag - 2068' and supplier_name='Valero' and
Account_Type='UNB Spot' and product_name='Unknown' ) or( en_terminal_name='VA Chesapeake -
Buckeye - 1650' and supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S -
UNB Fwrd Cont' and product_name='Gas - All' ) or( en_terminal_name='VA Chesapeake - Buckeye
- 1650' and supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB
Fwrd Cont' and product_name='Unknown' ) or( en_terminal_name='VA Chesapeake - Citgo - 1652'
and supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='PRM' ) or( en_terminal_name='VA Chesapeake - Citgo - 1652' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='ULS' ) or( en_terminal_name='VA Chesapeake - Citgo - 1652' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='UNL' ) or( en_terminal_name='VA Chesapeake - Citgo - 1652' and
supplier_name='Gulf' and Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and
product_name='Diesel Group' ) or( en_terminal_name='VA Chesapeake - Citgo - 1652' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='DSL - LSD/ULSD' ) or( en_terminal_name='VA Chesapeake - Citgo - 1652' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Gas - All' ) or( en_terminal_name='VA Chesapeake - Citgo - 1652' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Unknown' ) or( en_terminal_name='VA Chesapeake - KM - 1653' and
supplier_name='Gulf' and Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and
product_name='Diesel Group' ) or( en_terminal_name='VA Chesapeake - KM - 1654' and
supplier_name='Gulf' and Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and
product_name='GASOLINE GROUP' ) or( en_terminal_name='VA Chesapeake - TMG - 1656' and
supplier_name='TMG' and Account_Type='MANSFIELD OIL COMPANY OF (5738700)' and
product_name='TMGRFP - TMG PREMIUM RFG' ) or( en_terminal_name='VA Chesapeake - TMG - 1656'
and supplier_name='TMG' and Account_Type='MANSFIELD OIL COMPANY OF (5738700)' and
product_name='Unknown' ) or( en_terminal_name='VA Fairfax - Buckeye - 1659' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Gas - All' ) or( en_terminal_name='VA Fairfax - Buckeye - 1659' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Unknown' ) or( en_terminal_name='VA Fairfax - Citgo - 1661' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO-FORWARD PRICE O-342837' and
product_name='ULS' ) or( en_terminal_name='VA Fairfax - Citgo - 1661' and
supplier_name='Gulf' and Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and
product_name='Diesel Group' ) or( en_terminal_name='VA Fairfax - Citgo - 1661' and
supplier_name='Gulf' and Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and
product_name='GASOLINE GROUP' ) or( en_terminal_name='VA Fairfax - Citgo - 1661' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='DSL - LSD/ULSD' ) or( en_terminal_name='VA Fairfax - Citgo - 1661' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Gas - All' ) or( en_terminal_name='VA Fairfax - Citgo - 1661' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Unknown' ) or( en_terminal_name='VA Manassas - Sunoco - 1663' and
supplier_name='P66' and Account_Type='UB RACK' and product_name='CONVENTIONAL GASOLINE' )
or( en_terminal_name='VA Manassas - Sunoco - 1663' and supplier_name='P66' and
Account_Type='UB RACK' and product_name='DISTILLATES' ) or( en_terminal_name='VA Manassas -
Sunoco - 1663' and supplier_name='P66' and Account_Type='UB RACK' and product_name='O' ) or(
en_terminal_name='VA Manassas - Sunoco - 1663' and supplier_name='P66' and Account_Type='UB
RACK' and product_name='Q' ) or( en_terminal_name='VA Manassas - Sunoco - 1663' and
supplier_name='P66' and Account_Type='UB RACK' and product_name='R' ) or(
en_terminal_name='VA Montvale - Mag - 1668' and supplier_name='P66' and Account_Type='UB
RACK' and product_name='C' ) or( en_terminal_name='VA Montvale - Mag - 1668' and
supplier_name='P66' and Account_Type='UB RACK' and product_name='D' ) or(
en_terminal_name='VA Montvale - Mag - 1668' and supplier_name='P66' and Account_Type='UB
RACK' and product_name='DISTILLATES' ) or( en_terminal_name='VA Montvale - Mag - 1668' and
supplier_name='P66' and Account_Type='UB RACK' and product_name='P' ) or(
en_terminal_name='VA Newington - KM - 1671' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas - All' )
or( en_terminal_name='VA Richmond - Buckeye - 1677' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas - All' )
or( en_terminal_name='VA Richmond - Buckeye - 1677' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Unknown' )
or( en_terminal_name='VA Richmond - Citgo - 1679' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and product_name='PRM' ) or(
en_terminal_name='VA Richmond - Citgo - 1679' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and product_name='ULS' ) or(
en_terminal_name='VA Richmond - Citgo - 1679' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and product_name='UNL' ) or(
en_terminal_name='VA Richmond - Citgo - 1679' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO-FORWARD PRICE O-342837' and product_name='ULS' ) or(
en_terminal_name='VA Richmond - Citgo - 1679' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='DSL -
LSD/ULSD' ) or( en_terminal_name='VA Richmond - Citgo - 1679' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas - All' )
or( en_terminal_name='VA Richmond - Citgo - 1679' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Unknown' )
or( en_terminal_name='VA Richmond - KM - 1681' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and product_name='87 CONV E-10
GROUP' ) or( en_terminal_name='VA Richmond - KM - 1681' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and product_name='93 CONV E-10
GROUP' ) or( en_terminal_name='VA Richmond - KM - 1681' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and product_name='Diesel Group' )
or( en_terminal_name='VA Richmond - KM - 1681' and supplier_name='Gulf' and
Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and product_name='RFG PREMIUM
GASOLINE GROUP' ) or( en_terminal_name='VA Richmond - KM - 1681' and supplier_name='Gulf'
and Account_Type='MANSFIELD OIL CO OF GAINSVILLE - CONTRACT' and product_name='RFG REGULAR
GASOLINE GROUP' ) or( en_terminal_name='VA Richmond - KM - 1681' and supplier_name='Valero'
and Account_Type='MANSFIELD OIL CO O-120032-UB Rack' and product_name='DSL - LSD/ULSD' ) or(
en_terminal_name='VA Richmond - KM - 1681' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='DSL -
LSD/ULSD' ) or( en_terminal_name='VA Richmond - KM - 1681' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas - All' )
or( en_terminal_name='VA Richmond - KM - 1681' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas - All
Pre/Mid' ) or( en_terminal_name='VA Roanoke - KM - 1690' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Gas - All' )
or( en_terminal_name='IA Bettendorf - Buckeye - 3450' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='89 MID' ) or( en_terminal_name='IA Bettendorf -
Buckeye - 3450' and supplier_name='Murphy' and Account_Type='Unbranded' and product_name='93
PREMIUM' ) or( en_terminal_name='IA Bettendorf - Buckeye - 3450' and supplier_name='Murphy'
and Account_Type='Unbranded' and product_name='MIDGRADE' ) or( en_terminal_name='IA
Bettendorf - Buckeye - 3450' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='PREMIUM' ) or( en_terminal_name='IA Bettendorf - Buckeye - 3450' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='REGULAR' ) or(
en_terminal_name='IA Bettendorf - Buckeye - 3450' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='ULSD' ) or( en_terminal_name='IA Bettendorf -
Buckeye - 3450' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='Unknown' ) or( en_terminal_name='IL Alsip - Valero - 3300' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Unknown' ) or( en_terminal_name='IL Arlington Heights - Citgo - 3304' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='PRM' ) or( en_terminal_name='IL Arlington Heights - Citgo - 3304' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='UNL' ) or( en_terminal_name='IL Arlington Heights - Citgo - 3318' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='MID' ) or( en_terminal_name='IL Arlington Heights - Citgo - 3318' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='PRM' ) or( en_terminal_name='IL Arlington Heights - Citgo - 3318' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='ULS' ) or( en_terminal_name='IL Arlington Heights - Citgo - 3318' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='UNL' ) or( en_terminal_name='IL Hartford - BP - 3351' and supplier_name='BP'
and Account_Type='18 - Unbranded Jobbers' and product_name='MID-GRADE 9' ) or(
en_terminal_name='IL Hartford - BP - 3351' and supplier_name='BP' and Account_Type='18 -
Unbranded Jobbers' and product_name='MID-GRADE RFG' ) or( en_terminal_name='IL Hartford - BP
- 3351' and supplier_name='BP' and Account_Type='18 - Unbranded Jobbers' and
product_name='PREMIUM 9' ) or( en_terminal_name='IL Hartford - BP - 3351' and
supplier_name='BP' and Account_Type='18 - Unbranded Jobbers' and product_name='PREMIUM RFG'
) or( en_terminal_name='IL Hartford - BP - 3351' and supplier_name='BP' and Account_Type='18
- Unbranded Jobbers' and product_name='REGULAR 9' ) or( en_terminal_name='IL Hartford - BP -
3351' and supplier_name='BP' and Account_Type='18 - Unbranded Jobbers' and
product_name='REGULAR RFG' ) or( en_terminal_name='IL Lemont - Citgo - 3317' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='KUL' ) or( en_terminal_name='IL Lemont - Citgo - 3317' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='MID' ) or( en_terminal_name='IL Lemont - Citgo - 3317' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='PRM' ) or( en_terminal_name='IL Lemont - Citgo - 3317' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='ULS' ) or( en_terminal_name='IL Lemont - Citgo - 3317' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='UNL' ) or( en_terminal_name='IN Indianapolis - Buckeye - 3238' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Unknown' ) or( en_terminal_name='KY Lexington - Valero - 3267' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Unknown' ) or( en_terminal_name='KY Lexington - Valero - 3267' and
supplier_name='Valero' and Account_Type='UNB Spot' and product_name='Gas - Mid Conv' ) or(
en_terminal_name='KY Lexington - Valero - 3267' and supplier_name='Valero' and
Account_Type='UNB Spot' and product_name='Gas - Pre Conv' ) or( en_terminal_name='KY
Lexington - Valero - 3267' and supplier_name='Valero' and Account_Type='UNB Spot' and
product_name='Gas - Reg Conv' ) or( en_terminal_name='MI Ferrysburg - Citgo - 3008' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='ULS' ) or( en_terminal_name='MI Jackson - Citgo - 3009' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='ULS' ) or( en_terminal_name='MI Niles - Citgo - 3010' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='PRM' ) or( en_terminal_name='MI Niles - Citgo - 3010' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='ULS' ) or( en_terminal_name='MI Niles - Citgo - 3010' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='UNL' ) or( en_terminal_name='MI Romulus - MPC - 3034' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='ULS' ) or( en_terminal_name='MN St. Paul - Mag - 3415' and
supplier_name='Valero' and Account_Type='UNB Spot' and product_name='DSL - LSD/ULSD' ) or(
en_terminal_name='MN St. Paul - Mag - 3415' and supplier_name='Valero' and Account_Type='UNB
Spot' and product_name='Ethanol' ) or( en_terminal_name='OK Oklahoma City - P66 - 2612' and
supplier_name='Valero' and Account_Type='UNB Spot' and product_name='Gas - Pre Conv' ) or(
en_terminal_name='OK Oklahoma City - P66 - 2612' and supplier_name='Valero' and
Account_Type='UNB Spot' and product_name='Gas - Reg Conv' ) or( en_terminal_name='TN
Chattanooga - KM - 2201' and supplier_name='Chevron' and Account_Type='MANSFIELD OIL COMPANY
: AC5246494' and product_name='' ) or( en_terminal_name='TN Knoxville - Cummins - 2214' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Unknown' ) or( en_terminal_name='TN Knoxville - KM - 2215' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Unknown' ) or( en_terminal_name='TN Knoxville - Mag - 2219' and
supplier_name='P66' and Account_Type='UB RACK' and product_name='DISTILLATES' ) or(
en_terminal_name='TN Memphis - Valero - 2227' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Unknown' )
or( en_terminal_name='TN Memphis - Valero - 2227' and supplier_name='Valero' and
Account_Type='UNB Spot' and product_name='Gas - Mid Conv' ) or( en_terminal_name='TN Memphis
- Valero - 2227' and supplier_name='Valero' and Account_Type='UNB Spot' and
product_name='Gas - Pre Conv' ) or( en_terminal_name='TN Memphis - Valero - 2227' and
supplier_name='Valero' and Account_Type='UNB Spot' and product_name='Gas - Reg Conv' ) or(
en_terminal_name='TN Memphis - Valero - 2227' and supplier_name='Valero' and
Account_Type='UNB Spot' and product_name='Memphis 87 7.8#' ) or( en_terminal_name='TN
Memphis - Valero - 2227' and supplier_name='Valero' and Account_Type='UNB Spot' and
product_name='Memphis 89 7.8#' ) or( en_terminal_name='TN Memphis - Valero - 2227' and
supplier_name='Valero' and Account_Type='UNB Spot' and product_name='Memphis 93 7.8#' ) or(
en_terminal_name='TN Nashville - Cumberland - 2234' and supplier_name='P66' and
Account_Type='UB RACK' and product_name='Nashville Clear Regular CBOB w Pre' ) or(
en_terminal_name='TN Nashville - Cumberland - 2234' and supplier_name='P66' and
Account_Type='UB RACK' and product_name='Nashville REG 9.0 E10' ) or( en_terminal_name='TN
Nashville - Delek - 2204' and supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-
120032-W/S - UNB Fwrd Cont' and product_name='Gas - Mid Conv' ) or( en_terminal_name='TN
Nashville - Delek - 2204' and supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-
120032-W/S - UNB Fwrd Cont' and product_name='Gas - Pre Conv' ) or( en_terminal_name='TN
Nashville - Delek - 2204' and supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-
120032-W/S - UNB Fwrd Cont' and product_name='Gas - Reg Conv' ) or( en_terminal_name='TN
Nashville - Delek - 2204' and supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-
120032-W/S - UNB Fwrd Cont' and product_name='Unknown' ) or( en_terminal_name='TN Nashville
- Exxon - 2236' and supplier_name='Murphy' and Account_Type='Unbranded' and product_name='B'
) or( en_terminal_name='TN Nashville - Exxon - 2236' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='MIDGRADE' ) or( en_terminal_name='TN Nashville -
Exxon - 2236' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='PREMIUM' ) or( en_terminal_name='TN Nashville - Exxon - 2236' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='REGULAR' ) or(
en_terminal_name='TN Nashville - Exxon - 2236' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='ULSD' ) or( en_terminal_name='TN Nashville - Mag
- 2231' and supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531'
and product_name='MID' ) or( en_terminal_name='TN Nashville - Mag - 2231' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='PRM' ) or( en_terminal_name='TN Nashville - Mag - 2231' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='UNL' ) or( en_terminal_name='TN Nashville - Mag - 2231' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='93 PREMIUM' ) or(
en_terminal_name='TN Nashville - Mag - 2231' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='MIDGRADE' ) or( en_terminal_name='TN Nashville -
Mag - 2231' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='PREMIUM' ) or( en_terminal_name='TN Nashville - Mag - 2231' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='REGULAR' ) or(
en_terminal_name='TN Nashville - Mag - 2231' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='ULSD' ) or( en_terminal_name='TN Nashville - Mag
- 2231' and supplier_name='P66' and Account_Type='UB RACK' and product_name='D' ) or(
en_terminal_name='TN Nashville - Mag - 2231' and supplier_name='Shell' and
Account_Type='MANSFIELD OIL COMPANY OF 12313997' and product_name='Commercial Gas Product-
SH' ) or( en_terminal_name='TN Nashville - Mag - 2231' and supplier_name='Shell' and
Account_Type='MANSFIELD OIL COMPANY OF 12313997' and product_name='RUL 87 CG' ) or(
en_terminal_name='TN Nashville - Mag - 2240' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='93 PREMIUM' ) or( en_terminal_name='TN Nashville
- Mag - 2240' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='MIDGRADE' ) or( en_terminal_name='TN Nashville - Mag - 2240' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='PREMIUM' ) or(
en_terminal_name='TN Nashville - Mag - 2240' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='REGULAR' ) or( en_terminal_name='TN Nashville -
Mag - 2240' and supplier_name='Murphy' and Account_Type='Unbranded' and product_name='ULSD'
) or( en_terminal_name='TN Nashville - Mag - 2240' and supplier_name='Shell' and
Account_Type='MANSFIELD OIL COMPANY OF 12313997' and product_name='Commercial Gas Product-
SH' ) or( en_terminal_name='TN Nashville - Mag - 2240' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Unknown' )
or( en_terminal_name='TN Nashville - Motiva - 2241' and supplier_name='Shell' and
Account_Type='MANSFIELD OIL COMPANY OF 12313997' and product_name='Commercial Gas Product-
SH' ) or( en_terminal_name='WI Milwaukee - Buckeye - 3062' and supplier_name='BP' and
Account_Type='18 - Unbranded Jobbers' and product_name='MID-GRADE RFG' ) or(
en_terminal_name='WI Milwaukee - Buckeye - 3062' and supplier_name='BP' and Account_Type='18
- Unbranded Jobbers' and product_name='PREMIUM RFG' ) or( en_terminal_name='WI Milwaukee -
Buckeye - 3062' and supplier_name='BP' and Account_Type='18 - Unbranded Jobbers' and
product_name='REGULAR 9' ) or( en_terminal_name='WI Milwaukee - Buckeye - 3062' and
supplier_name='BP' and Account_Type='18 - Unbranded Jobbers' and product_name='REGULAR RFG'
) or( en_terminal_name='WI Milwaukee - Citgo - 3068' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and product_name='KUL' ) or(
en_terminal_name='WI Milwaukee - Citgo - 3068' and supplier_name='Citgo' and
Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and product_name='ULS' ) or(
en_terminal_name='AL Anniston - Murphy - 2333' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='87 HI RVP' ) or( en_terminal_name='AL Anniston -
Murphy - 2333' and supplier_name='Murphy' and Account_Type='Unbranded' and product_name='87
UNL CONV' ) or( en_terminal_name='AL Anniston - Murphy - 2333' and supplier_name='Murphy'
and Account_Type='Unbranded' and product_name='87 UNL E10' ) or( en_terminal_name='AL
Anniston - Murphy - 2333' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='89 HI RVP' ) or( en_terminal_name='AL Anniston - Murphy - 2333' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='89 MID CONV' ) or(
en_terminal_name='AL Anniston - Murphy - 2333' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='89 MID E10' ) or( en_terminal_name='AL Anniston -
Murphy - 2333' and supplier_name='Murphy' and Account_Type='Unbranded' and product_name='92
PREMIUM E10' ) or( en_terminal_name='AL Anniston - Murphy - 2333' and supplier_name='Murphy'
and Account_Type='Unbranded' and product_name='93 HI RVP' ) or( en_terminal_name='AL
Anniston - Murphy - 2333' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='93 PREMIUM CONV' ) or( en_terminal_name='AL Anniston - Murphy - 2333' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='93 PREMIUM E10' ) or(
en_terminal_name='AL Anniston - Murphy - 2333' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='OXY 87 E10 HI RVP' ) or( en_terminal_name='AL
Anniston - Murphy - 2333' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='OXY 89 E10 HI RVP' ) or( en_terminal_name='AL Anniston - Murphy - 2333' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='OXY 92 E10 HI RVP' )
or( en_terminal_name='AL Anniston - Murphy - 2333' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='OXY 93 E10 HI RVP' ) or( en_terminal_name='AL
Anniston - Murphy - 2333' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='ULSD' ) or( en_terminal_name='AL Anniston - Murphy - 2333' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='Unknown' ) or(
en_terminal_name='AL Birmingham - KM - 2307' and supplier_name='Valero' and
Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and product_name='Unknown' )
or( en_terminal_name='AL Birmingham - Mag - 2309' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='89 MID' ) or( en_terminal_name='AL Birmingham -
Mag - 2309' and supplier_name='Murphy' and Account_Type='Unbranded' and product_name='93
PREMIUM' ) or( en_terminal_name='AL Birmingham - Mag - 2309' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='MIDGRADE' ) or( en_terminal_name='AL Birmingham -
Mag - 2309' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='PREMIUM' ) or( en_terminal_name='AL Birmingham - Mag - 2309' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='ULSD' ) or(
en_terminal_name='AL Birmingham - Mag - 2309' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='Unknown' ) or( en_terminal_name='AL Birmingham -
Mag - 2309' and supplier_name='P66' and Account_Type='UB RACK' and product_name='D' ) or(
en_terminal_name='AL Birmingham - Mag - 2309' and supplier_name='P66' and Account_Type='UB
RACK' and product_name='DISTILLATES' ) or( en_terminal_name='AL Birmingham - Mag - 2309' and
supplier_name='P66' and Account_Type='UB RACK' and product_name='Unknown' ) or(
en_terminal_name='AL Sheffield - Murphy - 2335' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='87 HI RVP' ) or( en_terminal_name='AL Sheffield -
Murphy - 2335' and supplier_name='Murphy' and Account_Type='Unbranded' and product_name='87
UNL CONV' ) or( en_terminal_name='AL Sheffield - Murphy - 2335' and supplier_name='Murphy'
and Account_Type='Unbranded' and product_name='87 UNL E10' ) or( en_terminal_name='AL
Sheffield - Murphy - 2335' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='89 HI RVP' ) or( en_terminal_name='AL Sheffield - Murphy - 2335' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='89 MID CONV' ) or(
en_terminal_name='AL Sheffield - Murphy - 2335' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='89 MID E10' ) or( en_terminal_name='AL Sheffield
- Murphy - 2335' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='90 PREMIUM CONV' ) or( en_terminal_name='AL Sheffield - Murphy - 2335' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='92 HI RVP' ) or(
en_terminal_name='AL Sheffield - Murphy - 2335' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='92 PREMIUM CONV' ) or( en_terminal_name='AL
Sheffield - Murphy - 2335' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='92 PREMIUM E10' ) or( en_terminal_name='AL Sheffield - Murphy - 2335' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='93 PREMIUM E10' ) or(
en_terminal_name='AL Sheffield - Murphy - 2335' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='E-85' ) or( en_terminal_name='AL Sheffield -
Murphy - 2335' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='E85' ) or( en_terminal_name='AL Sheffield - Murphy - 2335' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='KEROSENE' ) or(
en_terminal_name='AL Sheffield - Murphy - 2335' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='OXY 87 E10 HI RVP' ) or( en_terminal_name='AL
Sheffield - Murphy - 2335' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='OXY 89 E10 HI RVP' ) or( en_terminal_name='AL Sheffield - Murphy - 2335' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='OXY 92 E10 HI RVP' )
or( en_terminal_name='AL Sheffield - Murphy - 2335' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='OXY 93 E10 HI RVP' ) or( en_terminal_name='AL
Sheffield - Murphy - 2335' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='P0M' ) or( en_terminal_name='AL Sheffield - Murphy - 2335' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='ULSD' ) or(
en_terminal_name='AL Sheffield - Murphy - 2335' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='Unknown' ) or( en_terminal_name='LA Arcadia -
Chevron - 2351' and supplier_name='Chevron' and Account_Type='MANSFIELD OIL COMPANY :
CHV5246494' and product_name='' ) or( en_terminal_name='LA Kenner - Motiva - 2365' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='MID' ) or( en_terminal_name='LA Kenner - Motiva - 2365' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='PRM' ) or( en_terminal_name='LA Kenner - Motiva - 2365' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='ULS' ) or( en_terminal_name='LA Kenner - Motiva - 2365' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='UNL' ) or( en_terminal_name='LA Lake Charles - Citgo - 2368' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='MID' ) or( en_terminal_name='LA Lake Charles - Citgo - 2368' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='PRM' ) or( en_terminal_name='LA Lake Charles - Citgo - 2368' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='ULS' ) or( en_terminal_name='LA Lake Charles - Citgo - 2368' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='UNL' ) or( en_terminal_name='MS Collins - Chevron - 2401' and
supplier_name='Chevron' and Account_Type='MANSFIELD OIL COMPANY : CHV5246494' and
product_name='' ) or( en_terminal_name='MS Collins - Chevron - 2401' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='93 PREMIUM' ) or(
en_terminal_name='MS Collins - Chevron - 2401' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='MIDGRADE' ) or( en_terminal_name='MS Collins -
Chevron - 2401' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='PREMIUM' ) or( en_terminal_name='MS Collins - Chevron - 2401' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='REGULAR' ) or(
en_terminal_name='MS Collins - Chevron - 2401' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='ULSD' ) or( en_terminal_name='MS Collins -
Chevron - 2401' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='Unknown' ) or( en_terminal_name='MS Collins - KM - 2402' and
supplier_name='Valero' and Account_Type='UNB Spot' and product_name='Gas - Pre Conv' ) or(
en_terminal_name='MS Collins - KM - 2402' and supplier_name='Valero' and Account_Type='UNB
Spot' and product_name='Gas - Reg Conv' ) or( en_terminal_name='MS Collins - TMG - 2405' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='93 PREMIUM' ) or(
en_terminal_name='MS Collins - TMG - 2405' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='MIDGRADE' ) or( en_terminal_name='MS Collins -
TMG - 2405' and supplier_name='Murphy' and Account_Type='Unbranded' and
product_name='PREMIUM' ) or( en_terminal_name='MS Collins - TMG - 2405' and
supplier_name='Murphy' and Account_Type='Unbranded' and product_name='REGULAR' ) or(
en_terminal_name='MS Collins - TMG - 2405' and supplier_name='Murphy' and
Account_Type='Unbranded' and product_name='ULSD' ) or( en_terminal_name='MS Collins - TMG -
2405' and supplier_name='Murphy' and Account_Type='Unbranded' and product_name='Unknown' )
or( en_terminal_name='MS Greenville - TMG - 2408' and supplier_name='Valero' and
Account_Type='UNB Spot' and product_name='Gas - Pre Conv' ) or( en_terminal_name='MS
Greenville - TMG - 2408' and supplier_name='Valero' and Account_Type='UNB Spot' and
product_name='Gas - Reg Conv' ) or( en_terminal_name='MS Pascagoula - Chevron - 2416' and
supplier_name='Chevron' and Account_Type='MANSFIELD OIL COMPANY : AC5246494' and
product_name='' ) or( en_terminal_name='MS Pascagoula - Chevron - 2416' and
supplier_name='Chevron' and Account_Type='MANSFIELD OIL COMPANY : CHV5246494' and
product_name='DIESEL #2' ) or( en_terminal_name='NM Albuquerque - Vecenergy - 4251' and
supplier_name='Chevron' and Account_Type='MANSFIELD OIL COMPANY : CHV5246494' and
product_name='' ) or( en_terminal_name='TX El Paso - Nustar - 2750' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Unknown' ) or( en_terminal_name='TX Grapevine - Nustar - 2680' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Unknown' ) or( en_terminal_name='TX San Antonio - Citgo - 2737' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='MID' ) or( en_terminal_name='TX San Antonio - Citgo - 2737' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='PRM' ) or( en_terminal_name='TX San Antonio - Citgo - 2737' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='ULS' ) or( en_terminal_name='TX San Antonio - Citgo - 2737' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='UNL' ) or( en_terminal_name='TX San Antonio - FHR - 2742' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Unknown' ) or( en_terminal_name='TX San Antonio - Nustar - 2738' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Unknown' ) or( en_terminal_name='TX San Antonio - Nustar - 2739' and
supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Fwrd Cont' and
product_name='Unknown' ) or( en_terminal_name='TX Victoria - Citgo - 2703' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='MID' ) or( en_terminal_name='TX Victoria - Citgo - 2703' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='PRM' ) or( en_terminal_name='TX Victoria - Citgo - 2703' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='ULS' ) or( en_terminal_name='TX Victoria - Citgo - 2703' and
supplier_name='Citgo' and Account_Type='MANSFIELD OIL CO OF GAINESVILLE--224531' and
product_name='UNL' ) or( en_terminal_name='CO Aurora - Mag - 4100' and supplier_name='P66'
and Account_Type='UB RACK' and product_name='JET' ) or( en_terminal_name='CO Aurora - Mag -
4100' and supplier_name='P66' and Account_Type='UB RACK' and product_name='Unknown' ) or(
en_terminal_name='UT North Salt Lake City - P66 - 4204' and supplier_name='P66' and
Account_Type='UB RACK' and product_name='D' ) or( en_terminal_name='UT North Salt Lake City
- P66 - 4204' and supplier_name='P66' and Account_Type='UB RACK' and
product_name='DISTILLATES' ) or( en_terminal_name='UT North Salt Lake City - P66 - 4204' and
supplier_name='P66' and Account_Type='UB RACK' and product_name='Unknown' ) or(
en_terminal_name='AZ Phoenix - Caljet - 4300' and supplier_name='Tesoro' and
Account_Type='UNBRANDED OPEN RACK' and product_name=' CA EPA ULSD' ) or(
en_terminal_name='AZ Phoenix - Caljet - 4300' and supplier_name='Tesoro' and
Account_Type='UNBRANDED OPEN RACK' and product_name='CA EPA ULSD' ) or( en_terminal_name='AZ
Phoenix - Caljet - 4300' and supplier_name='Tesoro' and Account_Type='UNBRANDED OPEN RACK'
and product_name='Unknown' ) or( en_terminal_name='AZ Phoenix - ProPetro - 4303' and
supplier_name='Tesoro' and Account_Type='UNBRANDED OPEN RACK' and product_name=' CA EPA
ULSD' ) or( en_terminal_name='AZ Phoenix - ProPetro - 4303' and supplier_name='Tesoro' and
Account_Type='UNBRANDED OPEN RACK' and product_name='CA EPA ULSD' ) or( en_terminal_name='AZ
Phoenix - ProPetro - 4303' and supplier_name='Tesoro' and Account_Type='UNBRANDED OPEN RACK'
and product_name='Unknown' ) or( en_terminal_name='AZ Phoenix - SFPP - 4304' and
supplier_name='Tesoro' and Account_Type='UNBRANDED OPEN RACK' and product_name=' CA EPA
ULSD' ) or( en_terminal_name='AZ Phoenix - SFPP - 4304' and supplier_name='Tesoro' and
Account_Type='UNBRANDED OPEN RACK' and product_name=' CA Non-Standard Products' ) or(
en_terminal_name='AZ Phoenix - SFPP - 4304' and supplier_name='Tesoro' and
Account_Type='UNBRANDED OPEN RACK' and product_name='CA EPA ULSD' ) or( en_terminal_name='AZ
Phoenix - SFPP - 4304' and supplier_name='Tesoro' and Account_Type='UNBRANDED OPEN RACK' and
product_name='CA Non-Standard Products' ) or( en_terminal_name='AZ Phoenix - SFPP - 4304'
and supplier_name='Valero' and Account_Type='MANSFIELD OIL CO O-120032-W/S - UNB Contract'
and product_name='Unknown' ) or( en_terminal_name='AZ Tucson - SFPP - 4310' and
supplier_name='P66' and Account_Type='UB RACK' and product_name='DISTILLATES' ) or(
en_terminal_name='AZ Tucson - SFPP - 4310' and supplier_name='Valero' and Account_Type='UNB
Spot' and product_name='DSL - LSD/ULSD' ) or( en_terminal_name='AZ Tucson - SFPP - 4310' and
supplier_name='Valero' and Account_Type='UNB Spot' and product_name='Gas - Pre Conv' ) or(
en_terminal_name='AZ Tucson - SFPP - 4310' and supplier_name='Valero' and Account_Type='UNB
Spot' and product_name='Gas - Reg Conv' ) or( en_terminal_name='AZ Tucson - SFPP - 4310' and
supplier_name='Valero' and Account_Type='UNB Spot' and product_name='KM Tucson Mid' ) or(
en_terminal_name='AZ Tucson - SFPP - 4310' and supplier_name='Valero' and Account_Type='UNB
Spot' and product_name='KM Tucson Prem' ) or( en_terminal_name='AZ Tucson - SFPP - 4310' and
supplier_name='Valero' and Account_Type='UNB Spot' and product_name='KM Tucson Reg' ) or(
en_terminal_name='CA Colton - SFPP - 4757' and supplier_name='Valero' and Account_Type='UNB
Spot' and product_name='DSL - LSD/ULSD' ) or( en_terminal_name='CA Brisbane - SFPP - 4700'
and supplier_name='Valero' and Account_Type='UNB Spot' and product_name='DSL - LSD/ULSD' )
or( en_terminal_name='CA Chico - SFPP - 4600' and supplier_name='Chevron' and
Account_Type='MANSFIELD OIL COMPANY : CHV5246494' and product_name='' ) or(
en_terminal_name='CA Chico - SFPP - 4600' and supplier_name='Chevron' and
Account_Type='MANSFIELD OIL COMPANY : CHV5246494' and product_name='Unknown' ) or(
en_terminal_name='CA Chico - SFPP - 4600' and supplier_name='P66' and Account_Type='UB RACK'
and product_name='' ) or( en_terminal_name='CA Chico - SFPP - 4600' and supplier_name='P66'
and Account_Type='UB RACK' and product_name='DISTILLATES' ) or( en_terminal_name='CA Chico -
SFPP - 4600' and supplier_name='P66' and Account_Type='UB RACK' and product_name='O' ) or(
en_terminal_name='CA Chico - SFPP - 4600' and supplier_name='P66' and Account_Type='UB RACK'
and product_name='P' ) or( en_terminal_name='CA Chico - SFPP - 4600' and supplier_name='P66'
and Account_Type='UB RACK' and product_name='R' ) or( en_terminal_name='CA Chico - SFPP -
4600' and supplier_name='Tesoro' and Account_Type='UNBRANDED OPEN RACK' and product_name=''
) or( en_terminal_name='CA Chico - SFPP - 4600' and supplier_name='Tesoro' and
Account_Type='UNBRANDED OPEN RACK' and product_name=' CA CARB ULSD' ) or(
en_terminal_name='CA Chico - SFPP - 4600' and supplier_name='Tesoro' and
Account_Type='UNBRANDED OPEN RACK' and product_name=' CA Mid & Pre Oxy Gasoline' ) or(
en_terminal_name='CA Chico - SFPP - 4600' and supplier_name='Tesoro' and
Account_Type='UNBRANDED OPEN RACK' and product_name=' CA Non-Standard Products' ) or(
en_terminal_name='CA Chico - SFPP - 4600' and supplier_name='Tesoro' and
Account_Type='UNBRANDED OPEN RACK' and product_name=' CA Reg Oxy Gasoline' ) or(
en_terminal_name='CA Chico - SFPP - 4600' and supplier_name='Tesoro' and
Account_Type='UNBRANDED OPEN RACK' and product_name='CA CARB ULSD' ) or(
en_terminal_name='CA Chico - SFPP - 4600' and supplier_name='Tesoro' and
Account_Type='UNBRANDED OPEN RACK' and product_name='CA Mid & Pre Oxy Gasoline' ) or(
en_terminal_name='CA Chico - SFPP - 4600' and supplier_name='Tesoro' and
Account_Type='UNBRANDED OPEN RACK' and product_name='CA Non-Standard Products' ) or(
en_terminal_name='CA Chico - SFPP - 4600' and supplier_name='Tesoro' and
Account_Type='UNBRANDED OPEN RACK' and product_name='CA Reg Oxy Gasoline' ) or(
en_terminal_name='CA Chico - SFPP - 4600' and supplier_name='Valero' and Account_Type='UNB
Spot' and product_name='DSL - LSD/ULSD' ) or( en_terminal_name='CA Chico - SFPP - 4600' and
supplier_name='Valero' and Account_Type='UNB Spot' and product_name='Unknown' ) or(
en_terminal_name='CA Fresno - SFPP - 4651' and supplier_name='Chevron' and
Account_Type='MANSFIELD OIL COMPANY : GL5246494' and product_name='DIESEL #2' ) or(
en_terminal_name='CA Montebella - Chevron - 4811' and supplier_name='Chevron' and
Account_Type='MANSFIELD OIL COMPANY : CHV5246494' and product_name='' ) or(
en_terminal_name='CA Montebella - Chevron - 4811' and supplier_name='Chevron' and
Account_Type='MANSFIELD OIL COMPANY : CHV5246494' and product_name='DIESEL #2' ) or(
en_terminal_name='NV Sparks - SFPP - 4353' and supplier_name='Valero' and Account_Type='UNB
Spot' and product_name='DSL - LSD/ULSD' ) or( en_terminal_name='NV Sparks - SFPP - 4353' and
supplier_name='Valero' and Account_Type='UNB Spot' and product_name='Gas - All' ) or(
en_terminal_name='0Unknown' and supplier_name='Valero' and Account_Type='MANSFIELD OIL CO
O-120032-W/S - UNB Contract' and product_name='DSL - LSD/ULSD' ) or(
en_terminal_name='0Unknown' and supplier_name='Valero' and Account_Type='UNB Spot' and
product_name='DSL - LSD/ULSD' ) or( en_terminal_name='0Unknown' and supplier_name='Valero'
and Account_Type='UNB Spot' and product_name='Gas - Pre Conv' ) or(
en_terminal_name='0Unknown' and supplier_name='Valero' and Account_Type='UNB Spot' and
product_name='Gas - Reg Conv' ) or( en_terminal_name='0Unknown' and supplier_name='BP' and
Account_Type='Unknown' and product_name='Denied' ) or( en_terminal_name='0Unknown' and
supplier_name='Exxon' and Account_Type='MANSFIELD OIL COMPANY OF 106305 IW' and
product_name='ULSD' ) or( en_terminal_name='0Unknown' and supplier_name='Husky' and
Account_Type='Contract' and product_name='87 Gas' ) or( en_terminal_name='0Unknown' and
supplier_name='Husky' and Account_Type='Contract' and product_name='93 Gas' ) or(
en_terminal_name='0Unknown' and supplier_name='Husky' and Account_Type='Contract' and
product_name='Diesel' ) or( en_terminal_name='0Unknown' and supplier_name='Husky' and
Account_Type='MANSFIELD GAS AT AKRON' and product_name='87 Gas' ) or(
en_terminal_name='0Unknown' and supplier_name='Husky' and Account_Type='MANSFIELD GAS AT
AKRON' and product_name='93 Gas' ) or( en_terminal_name='0Unknown' and supplier_name='Husky'
and Account_Type='MANSFIELD GAS AT AKRON' and product_name='Diesel' ) or(
en_terminal_name='0Unknown' and supplier_name='Husky' and Account_Type='MANSFIELD ULSD
CLEVE/AKRON' and product_name='Diesel' ) or(en_terminal_name='0Unknown' and
supplier_name='P66' and Account_Type='UB RACK' and product_name='GASOLINE'))) group by
en_terminal_name)"""
insertCartodb(insert)