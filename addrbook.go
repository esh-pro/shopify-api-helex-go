package main

import (
	"strings"
	"sync/atomic"
)

// ==========================================
// ADDRESS BOOK - multi-row country rotation (v15)
// ==========================================
//
// BookMulti: 196 countries x 3 researched rows. Every row is internally
// coherent (city <-> postal <-> zone agree; zone = ISO 3166-2 subdivision).
// Postal "" = country has no national postal system (AE, QA, AO...) -
// blank is correct; sending a value trips POSTAL_CODE_INVALID_FOR_COUNTRY.
// Rows rotate per pick so repeated checks never reuse one street fingerprint.
var BookMulti = map[string][]Address{
	"AD": {
		{"Carrer Major 14", "Andorra la Vella", "AD500", "07", "AD", ""},
		{"Avinguda Meritxell 32", "Andorra la Vella", "AD500", "07", "AD", ""},
		{"Carrer Prat de la Creu 6", "Andorra la Vella", "AD500", "07", "AD", ""},
	},
	"AE": {
		{"Sheikh Zayed Rd 14", "Dubai", "", "DU", "AE", ""},
		{"Corniche Rd 7", "Abu Dhabi", "", "AZ", "AE", ""},
		{"Al Khalidiyah St 22", "Abu Dhabi", "", "AZ", "AE", ""},
	},
	"AF": {
		{"Karte Parwan St 14", "Kabul", "1001", "KAB", "AF", ""},
		{"Wazir Akbar Khan St 7", "Kabul", "1001", "KAB", "AF", ""},
		{"Shahr-e-Naw St 22", "Kabul", "1006", "KAB", "AF", ""},
	},
	"AG": {
		{"Long Street 12", "Saint John's", "AG-0001", "01", "AG", ""},
		{"Nevis Street 7", "Saint John's", "AG-0001", "01", "AG", ""},
		{"High Street 3", "Saint John's", "AG-0001", "01", "AG", ""},
	},
	"AL": {
		{"Rruga Myslym Shyri 12", "Tirana", "1001", "11", "AL", ""},
		{"Rruga Ismail Qemali 5", "Tirana", "1019", "11", "AL", ""},
		{"Rruga 28 Nentori 3", "Durrës", "2001", "31", "AL", ""},
	},
	"AM": {
		{"Baghramyan Ave 15", "Yerevan", "0019", "08", "AM", ""},
		{"Mashtots Ave 27", "Yerevan", "0002", "08", "AM", ""},
		{"Tigranyan St 4", "Yerevan", "0010", "08", "AM", ""},
	},
	"AO": {
		{"Rua Rainha Ginga 17", "Luanda", "1000", "LUA", "AO", ""},
		{"Rua Comandante Gika 4", "Luanda", "1001", "LUA", "AO", ""},
		{"Rua Major Kanhangulo 9", "Luanda", "1002", "LUA", "AO", ""},
	},
	"AR": {
		{"Av. Corrientes 1234", "Buenos Aires", "C1043AAZ", "C", "AR", ""},
		{"Av. Santa Fe 892", "Buenos Aires", "C1059ABT", "C", "AR", ""},
		{"Calle Florida 345", "Buenos Aires", "C1005AAE", "C", "AR", ""},
	},
	"AT": {
		{"Mariahilfer Str 45", "Vienna", "1060", "9", "AT", ""},
		{"Naschmarktgasse 11", "Vienna", "1040", "9", "AT", ""},
		{"Linzer Gasse 18", "Salzburg", "5020", "5", "AT", ""},
	},
	"AU": {
		{"14 George St", "Sydney", "2000", "NSW", "AU", ""},
		{"22 Collins St", "Melbourne", "3000", "VIC", "AU", ""},
		{"9 St Georges Tce", "Perth", "6000", "WA", "AU", ""},
	},
	"AZ": {
		{"Neftchilar Ave 12", "Baku", "AZ1000", "BA", "AZ", ""},
		{"Istiqlaliyyat St 5", "Baku", "AZ1001", "BA", "AZ", ""},
		{"Rashid Behbudov St 9", "Baku", "AZ1005", "BA", "AZ", ""},
	},
	"BA": {
		{"Titova 12", "Sarajevo", "71000", "BIH", "BA", ""},
		{"Ferhadija 7", "Sarajevo", "71000", "BIH", "BA", ""},
		{"Marsala Tita 22", "Sarajevo", "71000", "BIH", "BA", ""},
	},
	"BB": {
		{"Broad St 14", "Bridgetown", "BB11000", "01", "BB", ""},
		{"Roebuck St 7", "Bridgetown", "BB11000", "01", "BB", ""},
		{"Swan St 22", "Bridgetown", "BB11000", "01", "BB", ""},
	},
	"BD": {
		{"Mirpur Rd 45", "Dhaka", "1216", "C", "BD", ""},
		{"Dhanmondi Rd 27", "Dhaka", "1209", "C", "BD", ""},
		{"Gulshan Ave 12", "Dhaka", "1212", "C", "BD", ""},
	},
	"BE": {
		{"Rue de la Loi 12", "Brussels", "1000", "BRU", "BE", ""},
		{"Avenue Louise 45", "Brussels", "1050", "BRU", "BE", ""},
		{"Rue Neuve 28", "Brussels", "1000", "BRU", "BE", ""},
	},
	"BF": {
		{"Ave Kwame Nkrumah 14", "Ouagadougou", "01 BP", "CEN", "BF", ""},
		{"Rue de la Chance 7", "Ouagadougou", "01 BP", "CEN", "BF", ""},
		{"Rue du Commerce 3", "Ouagadougou", "01 BP", "CEN", "BF", ""},
	},
	"BG": {
		{"Vitosha Blvd 45", "Sofia", "1000", "22", "BG", ""},
		{"Graf Ignatiev St 12", "Sofia", "1000", "22", "BG", ""},
		{"Patriarch Evtimii Blvd 8", "Sofia", "1000", "22", "BG", ""},
	},
	"BH": {
		{"Government Ave 12", "Manama", "304", "13", "BH", ""},
		{"Shaikh Isa Ave 6", "Manama", "305", "13", "BH", ""},
		{"Al Khalifa Ave 3", "Manama", "306", "13", "BH", ""},
	},
	"BI": {
		{"Chaussee du Prince 12", "Gitega", "", "GI", "BI", ""},
		{"Av. de la Jeunesse 7", "Gitega", "", "GI", "BI", ""},
		{"Rue de l'Hopital 4", "Gitega", "", "GI", "BI", ""},
	},
	"BJ": {
		{"Avenue Jean Paul II 8", "Cotonou", "", "AQ", "BJ", ""},
		{"Rue du Gouverneur 12", "Cotonou", "", "AQ", "BJ", ""},
		{"Boulevard de la Marina 5", "Cotonou", "", "AQ", "BJ", ""},
	},
	"BN": {
		{"Jalan Tutong 14", "Bandar Seri Begawan", "BB1110", "BM", "BN", ""},
		{"Jalan Sultan 7", "Bandar Seri Begawan", "BB1210", "BM", "BN", ""},
		{"Jalan McArthur 3", "Bandar Seri Begawan", "BB1710", "BM", "BN", ""},
	},
	"BO": {
		{"Calle Potosi 145", "La Paz", "", "L", "BO", ""},
		{"Av. 16 de Julio 234", "La Paz", "", "L", "BO", ""},
		{"Calle Comercio 67", "La Paz", "", "L", "BO", ""},
	},
	"BR": {
		{"Rua Augusta 1200", "São Paulo", "01304-001", "SP", "BR", ""},
		{"Av. Atlantica 500", "Rio de Janeiro", "22010-000", "RJ", "BR", ""},
		{"Asa Norte SHCN 305", "Brasília", "70736-520", "DF", "BR", ""},
	},
	"BS": {
		{"Bay St 22", "Nassau", "N-3045", "NP", "BS", ""},
		{"East Hill St 7", "Nassau", "N-1756", "NP", "BS", ""},
		{"West Bay St 14", "Nassau", "N-7776", "NP", "BS", ""},
	},
	"BT": {
		{"Norzin Lam 14", "Thimphu", "11001", "11", "BT", ""},
		{"Chang Lam 7", "Thimphu", "11001", "11", "BT", ""},
		{"Desi Lam 3", "Thimphu", "11001", "11", "BT", ""},
	},
	"BW": {
		{"Queens Rd 14", "Gaborone", "", "SE", "BW", ""},
		{"The Mall 7", "Gaborone", "", "SE", "BW", ""},
		{"Government Enclave 3", "Gaborone", "", "SE", "BW", ""},
	},
	"BY": {
		{"Nezavisimosti Ave 33", "Minsk", "220030", "HM", "BY", ""},
		{"Lenina St 19", "Minsk", "220050", "HM", "BY", ""},
		{"Komsomolskaya St 5", "Minsk", "220005", "HM", "BY", ""},
	},
	"BZ": {
		{"Albert St 14", "Belmopan", "", "CAY", "BZ", ""},
		{"Independence Blvd 7", "Belmopan", "", "CAY", "BZ", ""},
		{"Market Square 3", "Belize City", "", "BZ", "BZ", ""},
	},
	"CA": {
		{"123 Queen St W", "Toronto", "M5H 2M9", "ON", "CA", ""},
		{"845 Robson St", "Vancouver", "V6Z 1B3", "BC", "CA", ""},
		{"4200 Rue Saint-Denis", "Montreal", "H2J 2K8", "QC", "CA", ""},
	},
	"CD": {
		{"Av. des Huileries 14", "Kinshasa", "", "KN", "CD", ""},
		{"Bd. du 30 Juin 22", "Kinshasa", "", "KN", "CD", ""},
		{"Av. Kasavubu 5", "Kinshasa", "", "KN", "CD", ""},
	},
	"CF": {
		{"Av. David Dacko 14", "Bangui", "", "BGF", "CF", ""},
		{"Rue de la Mission 7", "Bangui", "", "BGF", "CF", ""},
		{"Boulevard de l'Indep 3", "Bangui", "", "BGF", "CF", ""},
	},
	"CG": {
		{"Av. Amilcar Cabral 14", "Brazzaville", "", "BZV", "CG", ""},
		{"Rue Behagle 7", "Brazzaville", "", "BZV", "CG", ""},
		{"Rue du General de Gaulle", "Brazzaville", "", "BZV", "CG", ""},
	},
	"CH": {
		{"Bahnhofstrasse 14", "Zurich", "8001", "ZH", "CH", ""},
		{"Rue du Rhone 7", "Geneva", "1204", "GE", "CH", ""},
		{"Kramgasse 22", "Bern", "3011", "BE", "CH", ""},
	},
	"CI": {
		{"Rue des Jardins 14", "Yamoussoukro", "", "YM", "CI", ""},
		{"Av. Houphouet Boigny 7", "Abidjan", "", "LG", "CI", ""},
		{"Rue du Commerce 3", "Abidjan", "", "LG", "CI", ""},
	},
	"CL": {
		{"Av. Libertador B. O'H 34", "Santiago", "8320000", "RM", "CL", ""},
		{"Calle Huérfanos 1189", "Santiago", "8340518", "RM", "CL", ""},
		{"Av. Providencia 2653", "Santiago", "7500011", "RM", "CL", ""},
	},
	"CM": {
		{"Rue Nachtigal 14", "Yaoundé", "", "CE", "CM", ""},
		{"Av. Kennedy 7", "Yaoundé", "", "CE", "CM", ""},
		{"Rue de l'Hôtel de Ville 3", "Yaoundé", "", "CE", "CM", ""},
	},
	"CN": {
		{"No. 1 Changan Ave", "Beijing", "100000", "BJ", "CN", ""},
		{"100 Nanjing Rd East", "Shanghai", "200001", "SH", "CN", ""},
		{"88 Renmin Rd", "Guangzhou", "510000", "GD", "CN", ""},
	},
	"CO": {
		{"Carrera 7 No 32-16", "Bogotá", "110311", "DC", "CO", ""},
		{"Calle 70 No 5-24", "Bogotá", "110231", "DC", "CO", ""},
		{"Carrera 43A No 1 Sur-100", "Medellín", "050001", "ANT", "CO", ""},
	},
	"CR": {
		{"Calle 1 Av. 2 No 123", "San José", "10101", "SJ", "CR", ""},
		{"Calle 3 Av. Central 45", "San José", "10102", "SJ", "CR", ""},
		{"Barrio Escalante 67", "San José", "10201", "SJ", "CR", ""},
	},
	"CU": {
		{"Calle Obispo 12", "Havana", "10100", "03", "CU", ""},
		{"Malecón 456", "Havana", "10200", "03", "CU", ""},
		{"Calle 23 No 789", "Havana", "10400", "03", "CU", ""},
	},
	"CV": {
		{"Avenida Amílcar Cabral 14", "Praia", "7600", "PR", "CV", ""},
		{"Rua 5 de Julho 7", "Praia", "7600", "PR", "CV", ""},
		{"Plateau 3", "Praia", "7600", "PR", "CV", ""},
	},
	"CY": {
		{"Archbishop Makarios III", "Nicosia", "1065", "01", "CY", ""},
		{"Ledra St 22", "Nicosia", "1011", "01", "CY", ""},
		{"Stasinou Ave 7", "Nicosia", "1060", "01", "CY", ""},
	},
	"CZ": {
		{"Václavské náměstí 14", "Prague", "110 00", "PR", "CZ", ""},
		{"Wenceslas Square 7", "Prague", "110 00", "PR", "CZ", ""},
		{"Na Příkopě 22", "Prague", "110 00", "PR", "CZ", ""},
	},
	"DE": {
		{"Unter den Linden 21", "Berlin", "10117", "BE", "DE", ""},
		{"Maximilianstr. 12", "Munich", "80539", "BY", "DE", ""},
		{"Zeil 106", "Frankfurt", "60313", "HE", "DE", ""},
	},
	"DJ": {
		{"Rue de Djibouti 12", "Djibouti City", "", "DJ", "DJ", ""},
		{"Boulevard de Gaule 7", "Djibouti City", "", "DJ", "DJ", ""},
		{"Rue de Venise 3", "Djibouti City", "", "DJ", "DJ", ""},
	},
	"DK": {
		{"Strøget 14", "Copenhagen", "1000", "84", "DK", ""},
		{"Nørrebrogade 22", "Copenhagen", "2200", "84", "DK", ""},
		{"Vesterbrogade 7", "Copenhagen", "1620", "84", "DK", ""},
	},
	"DM": {
		{"Great Marlborough St 5", "Roseau", "", "02", "DM", ""},
		{"Bath Rd 12", "Roseau", "", "02", "DM", ""},
		{"King George V St 7", "Roseau", "", "02", "DM", ""},
	},
	"DO": {
		{"Av. Winston Churchill 12", "Santo Domingo", "10101", "DN", "DO", ""},
		{"Calle El Conde 45", "Santo Domingo", "10210", "DN", "DO", ""},
		{"Av. Independencia 7", "Santo Domingo", "10103", "DN", "DO", ""},
	},
	"DZ": {
		{"Rue Didouche Mourad 14", "Algiers", "16000", "16", "DZ", ""},
		{"Rue Larbi Ben M'hidi 8", "Algiers", "16001", "16", "DZ", ""},
		{"Rue Mohamed Khemisti 21", "Oran", "31000", "31", "DZ", ""},
	},
	"EC": {
		{"Av. Amazonas N35-17", "Quito", "170150", "P", "EC", ""},
		{"Av. 9 de Octubre 100", "Guayaquil", "090150", "G", "EC", ""},
		{"Av. Colón 1234", "Quito", "170515", "P", "EC", ""},
	},
	"EE": {
		{"Viru 12", "Tallinn", "10140", "37", "EE", ""},
		{"Narva mnt 7", "Tallinn", "10117", "37", "EE", ""},
		{"Pärnu mnt 22", "Tallinn", "10141", "37", "EE", ""},
	},
	"EG": {
		{"Tahrir Square St 12", "Cairo", "11511", "C", "EG", ""},
		{"Ramses St 45", "Cairo", "11794", "C", "EG", ""},
		{"Corniche El Nil 7", "Cairo", "11221", "C", "EG", ""},
	},
	"ER": {
		{"Harnet Ave 12", "Asmara", "", "MA", "ER", ""},
		{"Martyrs Ave 7", "Asmara", "", "MA", "ER", ""},
		{"Liberation Ave 3", "Asmara", "", "MA", "ER", ""},
	},
	"ES": {
		{"Gran Vía 14", "Madrid", "28013", "MD", "ES", ""},
		{"Las Ramblas 7", "Barcelona", "08002", "CT", "ES", ""},
		{"Calle Sierpes 22", "Seville", "41004", "AN", "ES", ""},
	},
	"ET": {
		{"Bole Rd 45", "Addis Ababa", "1000", "AA", "ET", ""},
		{"Churchill Ave 22", "Addis Ababa", "1110", "AA", "ET", ""},
		{"Meskel Square 7", "Addis Ababa", "1000", "AA", "ET", ""},
	},
	"FI": {
		{"Mannerheimintie 14", "Helsinki", "00100", "01", "FI", ""},
		{"Aleksanterinkatu 22", "Helsinki", "00100", "01", "FI", ""},
		{"Fredrikinkatu 7", "Helsinki", "00120", "01", "FI", ""},
	},
	"FJ": {
		{"Victoria Parade 14", "Suva", "", "C", "FJ", ""},
		{"Renwick Rd 7", "Suva", "", "C", "FJ", ""},
		{"Cumming St 3", "Suva", "", "C", "FJ", ""},
	},
	"FM": {
		{"Main Rd 14", "Palikir", "96941", "PNI", "FM", ""},
		{"College Rd 7", "Palikir", "96941", "PNI", "FM", ""},
		{"Nett Rd 3", "Palikir", "96941", "PNI", "FM", ""},
	},
	"FR": {
		{"18 Rue de Rivoli", "Paris", "75004", "IDF", "FR", ""},
		{"7 Rue de la Paix", "Lyon", "69002", "ARA", "FR", ""},
		{"12 Rue Saint-Ferréol", "Marseille", "13001", "PAC", "FR", ""},
	},
	"GA": {
		{"Bd. Triomphal Omar Bongo", "Libreville", "", "LBV", "GA", ""},
		{"Rue des Chasses 7", "Libreville", "", "LBV", "GA", ""},
		{"Av. Colonel Parant 3", "Libreville", "", "LBV", "GA", ""},
	},
	"GB": {
		{"14 Ladbroke Grove", "London", "W11 3BQ", "ENG", "GB", ""},
		{"22 Byres Rd", "Glasgow", "G11 5RJ", "SCT", "GB", ""},
		{"9 Botanic Ave", "Belfast", "BT7 1JG", "NIR", "GB", ""},
	},
	"GD": {
		{"Church St 14", "St. George's", "", "01", "GD", ""},
		{"Granby St 7", "St. George's", "", "01", "GD", ""},
		{"Young St 3", "St. George's", "", "01", "GD", ""},
	},
	"GE": {
		{"Rustaveli Ave 14", "Tbilisi", "0108", "TB", "GE", ""},
		{"Chavchavadze Ave 7", "Tbilisi", "0179", "TB", "GE", ""},
		{"Kostava St 22", "Tbilisi", "0108", "TB", "GE", ""},
	},
	"GH": {
		{"Ring Rd Central 14", "Accra", "GA-002-1234", "AA", "GH", ""},
		{"Cantonments Rd 7", "Accra", "GA-044-3456", "AA", "GH", ""},
		{"Oxford St 22", "Accra", "GA-023-5678", "AA", "GH", ""},
	},
	"GM": {
		{"Kairaba Ave 14", "Banjul", "", "W", "GM", ""},
		{"Buckle St 7", "Banjul", "", "W", "GM", ""},
		{"Wellington St 3", "Banjul", "", "W", "GM", ""},
	},
	"GN": {
		{"Kaloum Corniche Rue 14", "Conakry", "", "C", "GN", ""},
		{"Almamya Ave 7", "Conakry", "", "C", "GN", ""},
		{"Boulevard du Commerce 3", "Conakry", "", "C", "GN", ""},
	},
	"GQ": {
		{"Calle 3 de Agosto 14", "Malabo", "", "BN", "GQ", ""},
		{"Av. de la Independencia 7", "Malabo", "", "BN", "GQ", ""},
		{"Calle Rey Boncoro 3", "Malabo", "", "BN", "GQ", ""},
	},
	"GR": {
		{"Ermou St 12", "Athens", "10563", "I", "GR", ""},
		{"Stadiou St 7", "Athens", "10562", "I", "GR", ""},
		{"Syntagma Square 3", "Athens", "10557", "I", "GR", ""},
	},
	"GT": {
		{"Calle 10 No 12", "Guatemala City", "01001", "GU", "GT", ""},
		{"7a Av. 6-30 Zona 1", "Guatemala City", "01001", "GU", "GT", ""},
		{"Blvd. Los Próceres 45", "Guatemala City", "01012", "GU", "GT", ""},
	},
	"GW": {
		{"Avenida Amílcar Cabral 7", "Bissau", "1000", "BS", "GW", ""},
		{"Rua Eduardo Mondlane 12", "Bissau", "1021", "BS", "GW", ""},
		{"Rua Justino Lopes 3", "Bissau", "1000", "BS", "GW", ""},
	},
	"GY": {
		{"Main St 14", "Georgetown", "", "DE", "GY", ""},
		{"Regent St 7", "Georgetown", "", "DE", "GY", ""},
		{"Church St 3", "Georgetown", "", "DE", "GY", ""},
	},
	"HN": {
		{"Blvd. Morazán 14", "Tegucigalpa", "11101", "FM", "HN", ""},
		{"Av. La Paz 7", "Tegucigalpa", "11102", "FM", "HN", ""},
		{"Calle Hipólito Matute 3", "Tegucigalpa", "11101", "FM", "HN", ""},
	},
	"HR": {
		{"Ilica 12", "Zagreb", "10000", "01", "HR", ""},
		{"Gajeva 7", "Zagreb", "10000", "01", "HR", ""},
		{"Trg Bana Jelacica 5", "Zagreb", "10000", "01", "HR", ""},
	},
	"HT": {
		{"Rue des Casernes 14", "Port-au-Prince", "HT6110", "OU", "HT", ""},
		{"Boulevard Toussaint 7", "Port-au-Prince", "HT6120", "OU", "HT", ""},
		{"Rue Pavée 3", "Port-au-Prince", "HT6111", "OU", "HT", ""},
	},
	"HU": {
		{"Andrássy út 45", "Budapest", "1062", "BU", "HU", ""},
		{"Váci utca 12", "Budapest", "1052", "BU", "HU", ""},
		{"Dohány utca 7", "Budapest", "1074", "BU", "HU", ""},
	},
	"ID": {
		{"Jl. Thamrin No 14", "Jakarta", "10310", "JK", "ID", ""},
		{"Jl. Sudirman No 7", "Jakarta", "10220", "JK", "ID", ""},
		{"Jl. Gatot Subroto 22", "Jakarta", "12930", "JK", "ID", ""},
	},
	"IE": {
		{"Grafton St 14", "Dublin", "D02 Y833", "L", "IE", ""},
		{"O'Connell St 7", "Dublin", "D01 K5F1", "L", "IE", ""},
		{"Pembroke Rd 22", "Dublin", "D04 XV28", "L", "IE", ""},
	},
	"IL": {
		{"Dizengoff St 14", "Tel Aviv", "6433203", "TA", "IL", ""},
		{"Ben Yehuda St 7", "Tel Aviv", "6340001", "TA", "IL", ""},
		{"Jaffa Rd 22", "Jerusalem", "9101001", "JM", "IL", ""},
	},
	"IN": {
		{"MG Rd 123", "Bangalore", "560001", "KA", "IN", ""},
		{"Linking Rd 45", "Mumbai", "400050", "MH", "IN", ""},
		{"Connaught Place 7", "New Delhi", "110001", "DL", "IN", ""},
	},
	"IQ": {
		{"Rashid St 14", "Baghdad", "10001", "BG", "IQ", ""},
		{"Karrada Dakhil 7", "Baghdad", "10011", "BG", "IQ", ""},
		{"Saadoun St 22", "Baghdad", "10064", "BG", "IQ", ""},
	},
	"IR": {
		{"Vali-e-Asr Ave 14", "Tehran", "1591634149", "23", "IR", ""},
		{"Keshavarz Blvd 7", "Tehran", "1416663111", "23", "IR", ""},
		{"Enghelab Ave 22", "Tehran", "1316834563", "23", "IR", ""},
	},
	"IS": {
		{"Laugavegur 14", "Reykjavik", "101", "1", "IS", ""},
		{"Bankastræti 7", "Reykjavik", "101", "1", "IS", ""},
		{"Skólavörðustígur 22", "Reykjavik", "101", "1", "IS", ""},
	},
	"IT": {
		{"Via del Corso 14", "Rome", "00186", "RM", "IT", ""},
		{"Via Montenapoleone 7", "Milan", "20121", "MI", "IT", ""},
		{"Via Toledo 22", "Naples", "80132", "NA", "IT", ""},
	},
	"JM": {
		{"Constant Spring Rd 14", "Kingston", "JMAKN07", "01", "JM", ""},
		{"King St 7", "Kingston", "JMAKN01", "01", "JM", ""},
		{"Half Way Tree Rd 22", "Kingston", "JMAKN04", "01", "JM", ""},
	},
	"JO": {
		{"Rainbow St 14", "Amman", "11118", "AZ", "JO", ""},
		{"Zahran St 7", "Amman", "11195", "AZ", "JO", ""},
		{"Wasfi Al Tal St 22", "Amman", "11953", "BA", "JO", ""},
	},
	"JP": {
		{"1-1 Marunouchi", "Tokyo", "100-0005", "13", "JP", ""},
		{"2-3 Shinsaibashisuji", "Osaka", "542-0085", "27", "JP", ""},
		{"3-1 Sakae", "Nagoya", "460-0008", "23", "JP", ""},
	},
	"KE": {
		{"Kenyatta Ave 14", "Nairobi", "00100", "30", "KE", ""},
		{"Moi Ave 7", "Nairobi", "00200", "30", "KE", ""},
		{"Uhuru Hwy 22", "Nairobi", "00100", "30", "KE", ""},
	},
	"KG": {
		{"Chui Ave 14", "Bishkek", "720001", "B", "KG", ""},
		{"Erkindik Blvd 7", "Bishkek", "720040", "B", "KG", ""},
		{"Manas Ave 22", "Bishkek", "720000", "B", "KG", ""},
	},
	"KH": {
		{"Monivong Blvd 45", "Phnom Penh", "12000", "12", "KH", ""},
		{"Norodom Blvd 22", "Phnom Penh", "12211", "12", "KH", ""},
		{"Street 278 No 14", "Phnom Penh", "12301", "12", "KH", ""},
	},
	"KI": {
		{"Bairiki Main Rd 5", "Tarawa", "", "G", "KI", ""},
		{"Betio Rd 12", "Tarawa", "", "G", "KI", ""},
		{"Bikenibeu Rd 3", "Tarawa", "", "G", "KI", ""},
	},
	"KM": {
		{"Rue de la Corniche 5", "Moroni", "", "G", "KM", ""},
		{"Bd. des Martyrs 12", "Moroni", "", "G", "KM", ""},
		{"Rue du Commerce 3", "Moroni", "", "G", "KM", ""},
	},
	"KN": {
		{"Central St 14", "Basseterre", "KN0101", "K", "KN", ""},
		{"Fort St 7", "Basseterre", "KN0101", "K", "KN", ""},
		{"Cayon St 3", "Basseterre", "KN0101", "K", "KN", ""},
	},
	"KP": {
		{"Sungri St 14", "Pyongyang", "", "01", "KP", ""},
		{"Kwangbok St 7", "Pyongyang", "", "01", "KP", ""},
		{"Mansudae Ave 3", "Pyongyang", "", "01", "KP", ""},
	},
	"KR": {
		{"Sejong-daero 14", "Seoul", "04519", "11", "KR", ""},
		{"Gangnam-daero 7", "Seoul", "06236", "11", "KR", ""},
		{"Teheran-ro 22", "Seoul", "06241", "11", "KR", ""},
	},
	"KW": {
		{"Salem Al Mubarak St 14", "Kuwait City", "20001", "KU", "KW", ""},
		{"Fahad Al Salem St 7", "Kuwait City", "13001", "KU", "KW", ""},
		{"Gulf Rd 22", "Kuwait City", "32001", "HA", "KW", ""},
	},
	"KZ": {
		{"Nurzhol Blvd 14", "Astana", "010000", "AKM", "KZ", ""},
		{"Republic Ave 7", "Almaty", "050010", "ALM", "KZ", ""},
		{"Prospect Abaya 22", "Almaty", "050000", "ALM", "KZ", ""},
	},
	"LA": {
		{"Samsenthai Rd 14", "Vientiane", "01000", "VT", "LA", ""},
		{"Setthathirath Rd 7", "Vientiane", "01001", "VT", "LA", ""},
		{"Nokeokoummane Rd 3", "Vientiane", "01002", "VT", "LA", ""},
	},
	"LB": {
		{"Hamra St 14", "Beirut", "1103", "BA", "LB", ""},
		{"Bliss St 7", "Beirut", "1107", "BA", "LB", ""},
		{"Verdun St 22", "Beirut", "2034", "BA", "LB", ""},
	},
	"LC": {
		{"Bridge St 14", "Castries", "LC01 101", "10", "LC", ""},
		{"Micoud St 7", "Castries", "LC01 201", "10", "LC", ""},
		{"Jeremie St 3", "Castries", "LC01 101", "10", "LC", ""},
	},
	"LI": {
		{"Städtle 14", "Vaduz", "9490", "11", "LI", ""},
		{"Aeulestrasse 7", "Vaduz", "9490", "11", "LI", ""},
		{"Peter Kaiser Platz 3", "Vaduz", "9490", "11", "LI", ""},
	},
	"LK": {
		{"Galle Rd 14", "Colombo", "00300", "11", "LK", ""},
		{"Duplication Rd 7", "Colombo", "00300", "11", "LK", ""},
		{"Baudhaloka Mawatha 22", "Colombo", "00700", "11", "LK", ""},
	},
	"LR": {
		{"Broad St 14", "Monrovia", "", "BM", "LR", ""},
		{"Randall St 7", "Monrovia", "", "BM", "LR", ""},
		{"Capitol Hill 3", "Monrovia", "", "BM", "LR", ""},
	},
	"LS": {
		{"Kingsway Rd 14", "Maseru", "100", "A", "LS", ""},
		{"Constitution Rd 7", "Maseru", "100", "A", "LS", ""},
		{"Pitso Ground Rd 3", "Maseru", "100", "A", "LS", ""},
	},
	"LT": {
		{"Gedimino pr. 14", "Vilnius", "01103", "VL", "LT", ""},
		{"Pilies g. 7", "Vilnius", "01123", "VL", "LT", ""},
		{"Vokieciu g. 22", "Vilnius", "01130", "VL", "LT", ""},
	},
	"LU": {
		{"Boulevard Royal 14", "Luxembourg", "2449", "LU", "LU", ""},
		{"Rue de la Poste 7", "Luxembourg", "2346", "LU", "LU", ""},
		{"Avenue de la Gare 22", "Luxembourg", "1610", "LU", "LU", ""},
	},
	"LV": {
		{"Brivibas iela 14", "Riga", "LV-1001", "RIX", "LV", ""},
		{"Kalku iela 7", "Riga", "LV-1050", "RIX", "LV", ""},
		{"Dzirnavu iela 22", "Riga", "LV-1010", "RIX", "LV", ""},
	},
	"LY": {
		{"Rashid St 14", "Tripoli", "", "TB", "LY", ""},
		{"Shara Omar Mukhtar 7", "Tripoli", "", "TB", "LY", ""},
		{"Shara al Jumhuriya 3", "Tripoli", "", "TB", "LY", ""},
	},
	"MA": {
		{"Av. Mohammed V 14", "Rabat", "10000", "04", "MA", ""},
		{"Boulevard Hassan II 7", "Casablanca", "20000", "01", "MA", ""},
		{"Rue de la Paix 22", "Marrakech", "40000", "09", "MA", ""},
	},
	"MC": {
		{"Avenue de la Costa 14", "Monaco", "98000", "MC", "MC", ""},
		{"Boulevard des Moulins 7", "Monaco", "98000", "MC", "MC", ""},
		{"Rue Grimaldi 3", "Monaco", "98000", "MC", "MC", ""},
	},
	"MD": {
		{"Stefan cel Mare 14", "Chișinău", "MD-2001", "CU", "MD", ""},
		{"Puskin St 7", "Chișinău", "MD-2012", "CU", "MD", ""},
		{"Alecu Russo Blvd 22", "Chișinău", "MD-2068", "CU", "MD", ""},
	},
	"ME": {
		{"Bulevar Svetog Petra 14", "Podgorica", "81000", "16", "ME", ""},
		{"Slobode Blvd 7", "Podgorica", "81000", "16", "ME", ""},
		{"Revolucije Blvd 3", "Podgorica", "81000", "16", "ME", ""},
	},
	"MG": {
		{"Lalana Rainizanamanga 14", "Antananarivo", "101", "T", "MG", ""},
		{"Rue Ranavalona III 7", "Antananarivo", "101", "T", "MG", ""},
		{"Lalana Solombavambahoaka", "Antananarivo", "102", "T", "MG", ""},
	},
	"MH": {
		{"Delap Rd 14", "Majuro", "96960", "MAJ", "MH", ""},
		{"Rita Rd 7", "Majuro", "96960", "MAJ", "MH", ""},
		{"Uliga Rd 3", "Majuro", "96960", "MAJ", "MH", ""},
	},
	"MK": {
		{"Makedonija Blvd 14", "Skopje", "1000", "84", "MK", ""},
		{"Dame Gruev St 7", "Skopje", "1000", "84", "MK", ""},
		{"Marshal Tito Sq 3", "Skopje", "1000", "84", "MK", ""},
	},
	"ML": {
		{"Rue Soundiata Keita 14", "Bamako", "", "BKO", "ML", ""},
		{"Av. de l'Yser 7", "Bamako", "", "BKO", "ML", ""},
		{"Boulevard du Peuple 3", "Bamako", "", "BKO", "ML", ""},
	},
	"MM": {
		{"Pyay Rd 14", "Naypyidaw", "15011", "NPT", "MM", ""},
		{"Nay Pyi Taw Rd 7", "Naypyidaw", "15011", "NPT", "MM", ""},
		{"Uppatasanti Township 3", "Naypyidaw", "15012", "NPT", "MM", ""},
	},
	"MN": {
		{"Enkhtaivan Ave 14", "Ulaanbaatar", "14210", "1", "MN", ""},
		{"Sukhbaatar Square 7", "Ulaanbaatar", "15160", "1", "MN", ""},
		{"Peace Ave 22", "Ulaanbaatar", "14250", "1", "MN", ""},
	},
	"MR": {
		{"Rue Mamadou Konaté 14", "Nouakchott", "", "NKC", "MR", ""},
		{"Av. du Général de Gaulle", "Nouakchott", "", "NKC", "MR", ""},
		{"Rue Kennedy 3", "Nouakchott", "", "NKC", "MR", ""},
	},
	"MT": {
		{"Republic St 14", "Valletta", "VLT 1110", "01", "MT", ""},
		{"Merchants St 7", "Valletta", "VLT 1174", "01", "MT", ""},
		{"Old Bakery St 3", "Valletta", "VLT 1453", "01", "MT", ""},
	},
	"MU": {
		{"Royal Rd 14", "Port Louis", "11324", "PL", "MU", ""},
		{"Jules Koenig St 7", "Port Louis", "11101", "PL", "MU", ""},
		{"Farquhar St 3", "Port Louis", "11001", "PL", "MU", ""},
	},
	"MV": {
		{"Majeedhee Magu 14", "Malé", "20026", "MLE", "MV", ""},
		{"Orchid Magu 7", "Malé", "20184", "MLE", "MV", ""},
		{"Ameeru Ahmed Magu 3", "Malé", "20125", "MLE", "MV", ""},
	},
	"MW": {
		{"Victoria Ave 14", "Lilongwe", "", "LI", "MW", ""},
		{"Independence Dr 7", "Lilongwe", "", "LI", "MW", ""},
		{"Capital Hill 3", "Lilongwe", "", "LI", "MW", ""},
	},
	"MX": {
		{"Av. Reforma 222", "Mexico City", "06600", "CDMX", "MX", ""},
		{"Calle Madero 45", "Mexico City", "06000", "CDMX", "MX", ""},
		{"Av. Juárez 7", "Mexico City", "06010", "CDMX", "MX", ""},
	},
	"MY": {
		{"Jalan Bukit Bintang 14", "Kuala Lumpur", "55100", "14", "MY", ""},
		{"Jalan Ampang 7", "Kuala Lumpur", "50450", "14", "MY", ""},
		{"Jalan Tuanku Abdul Halim", "Kuala Lumpur", "50480", "14", "MY", ""},
	},
	"MZ": {
		{"Av. 25 de Setembro 14", "Maputo", "1100", "MPM", "MZ", ""},
		{"Av. Samora Machel 7", "Maputo", "1101", "MPM", "MZ", ""},
		{"Rua da Resistência 3", "Maputo", "1100", "MPM", "MZ", ""},
	},
	"NA": {
		{"Independence Ave 14", "Windhoek", "10001", "KH", "NA", ""},
		{"Sam Nujoma Dr 7", "Windhoek", "10003", "KH", "NA", ""},
		{"Robert Mugabe Ave 22", "Windhoek", "10004", "KH", "NA", ""},
	},
	"NE": {
		{"Rue de l'Uranium 14", "Niamey", "", "8", "NE", ""},
		{"Boulevard Mali Bero 7", "Niamey", "", "8", "NE", ""},
		{"Rue des Bâtisseurs 3", "Niamey", "", "8", "NE", ""},
	},
	"NG": {
		{"Ahmadu Bello Way 14", "Abuja", "900001", "FC", "NG", ""},
		{"Tafawa Balewa Square 7", "Lagos", "101001", "LA", "NG", ""},
		{"Adetokunbo Ademola 22", "Abuja", "900211", "FC", "NG", ""},
	},
	"NI": {
		{"Calle 27 de Mayo 14", "Managua", "11001", "MN", "NI", ""},
		{"Av. Bolivar 7", "Managua", "11002", "MN", "NI", ""},
		{"Pista Juan Pablo II 3", "Managua", "11003", "MN", "NI", ""},
	},
	"NL": {
		{"Damrak 14", "Amsterdam", "1012 LP", "NH", "NL", ""},
		{"Kalverstraat 7", "Amsterdam", "1012 NX", "NH", "NL", ""},
		{"Binnenhof 22", "The Hague", "2513 AA", "ZH", "NL", ""},
	},
	"NO": {
		{"Karl Johans gate 14", "Oslo", "0154", "03", "NO", ""},
		{"Bogstadveien 7", "Oslo", "0355", "03", "NO", ""},
		{"Drammensveien 22", "Oslo", "0255", "03", "NO", ""},
	},
	"NP": {
		{"New Rd 14", "Kathmandu", "44600", "3", "NP", ""},
		{"Kantipath 7", "Kathmandu", "44600", "3", "NP", ""},
		{"Durbar Marg 22", "Kathmandu", "44600", "3", "NP", ""},
	},
	"NR": {
		{"Aiwo District Rd 5", "Yaren", "", "14", "NR", ""},
		{"Meneng Rd 12", "Yaren", "", "14", "NR", ""},
		{"Buada Rd 3", "Yaren", "", "14", "NR", ""},
	},
	"NZ": {
		{"Queen St 14", "Auckland", "1010", "AUK", "NZ", ""},
		{"Lambton Quay 7", "Wellington", "6011", "WGN", "NZ", ""},
		{"Cathedral Square 22", "Christchurch", "8011", "CAN", "NZ", ""},
	},
	"OM": {
		{"Muttrah Corniche 14", "Muscat", "113", "MA", "OM", ""},
		{"Sultan Qaboos St 7", "Muscat", "114", "MA", "OM", ""},
		{"Al Qurum St 22", "Muscat", "118", "MA", "OM", ""},
	},
	"PA": {
		{"Calle 50 No 14", "Panama City", "0816", "8", "PA", ""},
		{"Via España 7", "Panama City", "0819", "8", "PA", ""},
		{"Av. Balboa 22", "Panama City", "0818", "8", "PA", ""},
	},
	"PE": {
		{"Av. Larco 14", "Lima", "15074", "LIM", "PE", ""},
		{"Av. Javier Prado 7", "Lima", "15036", "LIM", "PE", ""},
		{"Jr. de la Unión 22", "Lima", "15001", "LIM", "PE", ""},
	},
	"PG": {
		{"Waigani Dr 14", "Port Moresby", "121", "NCD", "PG", ""},
		{"Ela Beach Rd 7", "Port Moresby", "111", "NCD", "PG", ""},
		{"Boroko Rd 3", "Port Moresby", "111", "NCD", "PG", ""},
	},
	"PH": {
		{"Ayala Ave 14", "Makati", "1226", "00", "PH", ""},
		{"Roxas Blvd 7", "Manila", "1000", "00", "PH", ""},
		{"EDSA 22", "Quezon City", "1100", "00", "PH", ""},
	},
	"PK": {
		{"Shahrah-e-Faisal 14", "Karachi", "75350", "SD", "PK", ""},
		{"The Mall 7", "Lahore", "54000", "PB", "PK", ""},
		{"Jinnah Ave 22", "Islamabad", "44000", "IS", "PK", ""},
	},
	"PL": {
		{"Krakowskie Przedmiescie 14", "Warsaw", "00-325", "14", "PL", ""},
		{"Nowy Swiat 7", "Warsaw", "00-497", "14", "PL", ""},
		{"ul. Florianska 22", "Kraków", "31-019", "12", "PL", ""},
	},
	"PS": {
		{"Al Irsal St 14", "Ramallah", "", "RBH", "PS", ""},
		{"King Faisal St 7", "Gaza", "", "GZA", "PS", ""},
		{"Nablus Rd 3", "Ramallah", "", "RBH", "PS", ""},
	},
	"PT": {
		{"Avenida da Liberdade 14", "Lisbon", "1250-096", "11", "PT", ""},
		{"Rua Augusta 7", "Lisbon", "1100-053", "11", "PT", ""},
		{"Rua de Santa Catarina 22", "Porto", "4000-447", "13", "PT", ""},
	},
	"PW": {
		{"Koror Rd 14", "Ngerulmud", "96940", "150", "PW", ""},
		{"Topside Rd 7", "Ngerulmud", "96940", "150", "PW", ""},
		{"Capitol Hill 3", "Ngerulmud", "96940", "150", "PW", ""},
	},
	"PY": {
		{"Av. Mariscal Lopez 14", "Asunción", "1209", "ASU", "PY", ""},
		{"Calle Palma 7", "Asunción", "1001", "ASU", "PY", ""},
		{"Av. España 22", "Asunción", "1001", "ASU", "PY", ""},
	},
	"QA": {
		{"Al Corniche St 14", "Doha", "", "DA", "QA", ""},
		{"Al Sadd St 7", "Doha", "", "DA", "QA", ""},
		{"Salwa Rd 22", "Doha", "", "DA", "QA", ""},
	},
	"RO": {
		{"Calea Victoriei 14", "Bucharest", "010063", "B", "RO", ""},
		{"Bulevardul Unirii 7", "Bucharest", "040107", "B", "RO", ""},
		{"Strada Lipscani 22", "Bucharest", "030033", "B", "RO", ""},
	},
	"RS": {
		{"Knez Mihailova 14", "Belgrade", "11000", "00", "RS", ""},
		{"Terazije 7", "Belgrade", "11000", "00", "RS", ""},
		{"Kralja Milana 22", "Belgrade", "11000", "00", "RS", ""},
	},
	"RU": {
		{"Tverskaya St 14", "Moscow", "125009", "MOW", "RU", ""},
		{"Nevsky Prospekt 7", "St Petersburg", "191186", "SPE", "RU", ""},
		{"Lenina St 22", "Novosibirsk", "630099", "NVS", "RU", ""},
	},
	"RW": {
		{"KN 3 Ave 14", "Kigali", "", "01", "RW", ""},
		{"KG 7 Ave 7", "Kigali", "", "01", "RW", ""},
		{"KN 5 Rd 22", "Kigali", "", "01", "RW", ""},
	},
	"SA": {
		{"King Fahd Rd 14", "Riyadh", "11564", "01", "SA", ""},
		{"Olaya St 7", "Riyadh", "12213", "01", "SA", ""},
		{"Tahlia St 22", "Jeddah", "23521", "02", "SA", ""},
	},
	"SB": {
		{"Mendana Ave 14", "Honiara", "", "CT", "SB", ""},
		{"Hibiscus Ave 7", "Honiara", "", "CT", "SB", ""},
		{"Rove Rd 3", "Honiara", "", "CT", "SB", ""},
	},
	"SC": {
		{"Victoria Ave 14", "Victoria", "", "25", "SC", ""},
		{"Albert St 7", "Victoria", "", "25", "SC", ""},
		{"State House Ave 3", "Victoria", "", "25", "SC", ""},
	},
	"SD": {
		{"Al Qasr Ave 14", "Khartoum", "11111", "KH", "SD", ""},
		{"Shari Gamma Ave 7", "Khartoum", "11214", "KH", "SD", ""},
		{"Africa Rd 22", "Khartoum", "11042", "KH", "SD", ""},
	},
	"SE": {
		{"Drottninggatan 14", "Stockholm", "111 51", "AB", "SE", ""},
		{"Kungsportsavenyen 7", "Gothenburg", "411 36", "O", "SE", ""},
		{"Stortorget 22", "Malmö", "211 22", "M", "SE", ""},
	},
	"SG": {
		{"Orchard Rd 14", "Singapore", "238841", "SG", "SG", ""},
		{"Marina Bay Ave 7", "Singapore", "018940", "SG", "SG", ""},
		{"Shenton Way 22", "Singapore", "068803", "SG", "SG", ""},
	},
	"SI": {
		{"Slovenska cesta 14", "Ljubljana", "1000", "061", "SI", ""},
		{"Cankarjeva 7", "Ljubljana", "1000", "061", "SI", ""},
		{"Stritarjeva 22", "Ljubljana", "1000", "061", "SI", ""},
	},
	"SK": {
		{"Obchodná 14", "Bratislava", "811 06", "BL", "SK", ""},
		{"Laurinská 7", "Bratislava", "811 01", "BL", "SK", ""},
		{"Ventúrska 22", "Bratislava", "811 01", "BL", "SK", ""},
	},
	"SL": {
		{"Siaka Stevens St 14", "Freetown", "", "W", "SL", ""},
		{"Wilberforce St 7", "Freetown", "", "W", "SL", ""},
		{"Lumley Beach Rd 3", "Freetown", "", "W", "SL", ""},
	},
	"SM": {
		{"Contrada Omerelli 14", "San Marino", "47890", "07", "SM", ""},
		{"Via Delle Carrare 7", "San Marino", "47890", "07", "SM", ""},
		{"Piazza della Libertà 3", "San Marino", "47890", "07", "SM", ""},
	},
	"SN": {
		{"Av. Léopold S. Senghor 14", "Dakar", "12500", "DK", "SN", ""},
		{"Rue de Thiong 7", "Dakar", "11000", "DK", "SN", ""},
		{"Boulevard de la Republic", "Dakar", "11000", "DK", "SN", ""},
	},
	"SO": {
		{"Maka Al Mukarramah Rd 14", "Mogadishu", "", "BN", "SO", ""},
		{"Via Roma 7", "Mogadishu", "", "BN", "SO", ""},
		{"Jidhi Rd 3", "Mogadishu", "", "BN", "SO", ""},
	},
	"SR": {
		{"Waterkant 14", "Paramaribo", "", "PR", "SR", ""},
		{"Domineestraat 7", "Paramaribo", "", "PR", "SR", ""},
		{"Henck Arronstraat 22", "Paramaribo", "", "PR", "SR", ""},
	},
	"SS": {
		{"Juba Rd 14", "Juba", "", "EE", "SS", ""},
		{"Airport Rd 7", "Juba", "", "EE", "SS", ""},
		{"Hai Jabel Rd 3", "Juba", "", "EE", "SS", ""},
	},
	"ST": {
		{"Av. Marginal 12 de Julho", "São Tomé", "", "ME", "ST", ""},
		{"Rua Conceição 7", "São Tomé", "", "ME", "ST", ""},
		{"Av. da Independência 3", "São Tomé", "", "ME", "ST", ""},
	},
	"SV": {
		{"Calle Arce 12", "San Salvador", "01101", "SS", "SV", ""},
		{"Blvd. Los Héroes 45", "San Salvador", "01101", "SS", "SV", ""},
		{"Col. Escalón 7", "San Salvador", "01101", "SS", "SV", ""},
	},
	"SY": {
		{"Al Hamra St 14", "Damascus", "", "DI", "SY", ""},
		{"Maysaloun St 7", "Damascus", "", "DI", "SY", ""},
		{"Shoukri Al Quwatli Ave 3", "Damascus", "", "DI", "SY", ""},
	},
	"SZ": {
		{"Msunduza Rd 14", "Mbabane", "H100", "HH", "SZ", ""},
		{"Gwamile St 7", "Mbabane", "H100", "HH", "SZ", ""},
		{"Mhlambanyatsi Rd 3", "Mbabane", "H100", "HH", "SZ", ""},
	},
	"TD": {
		{"Rue du 30 Aout 14", "N'Djamena", "", "ND", "TD", ""},
		{"Av. Charles de Gaulle 7", "N'Djamena", "", "ND", "TD", ""},
		{"Rue Joseph Brahim 3", "N'Djamena", "", "ND", "TD", ""},
	},
	"TG": {
		{"Rue du Commerce 14", "Lomé", "", "C", "TG", ""},
		{"Av. 24 Janvier 7", "Lomé", "", "C", "TG", ""},
		{"Bd. du 13 Janvier 3", "Lomé", "", "C", "TG", ""},
	},
	"TH": {
		{"Silom Rd 14", "Bangkok", "10500", "10", "TH", ""},
		{"Sukhumvit Rd 7", "Bangkok", "10110", "10", "TH", ""},
		{"Ratchadamri Rd 22", "Bangkok", "10330", "10", "TH", ""},
	},
	"TJ": {
		{"Rudaki Ave 14", "Dushanbe", "734025", "DU", "TJ", ""},
		{"Ismoil Somoni Ave 7", "Dushanbe", "734003", "DU", "TJ", ""},
		{"Bokhtar St 22", "Dushanbe", "734001", "DU", "TJ", ""},
	},
	"TL": {
		{"Av. de Portugal 14", "Dili", "", "DI", "TL", ""},
		{"Rua Quinze de Outubro 7", "Dili", "", "DI", "TL", ""},
		{"Av. dos Direitos Humanos", "Dili", "", "DI", "TL", ""},
	},
	"TM": {
		{"Bitarap Turkmenistan Ave", "Ashgabat", "744000", "A", "TM", ""},
		{"Garaşsyzlyk Ave 7", "Ashgabat", "744001", "A", "TM", ""},
		{"Saparmyrat Turkmenbaşy 3", "Ashgabat", "744000", "A", "TM", ""},
	},
	"TN": {
		{"Avenue Habib Bourguiba 14", "Tunis", "1001", "11", "TN", ""},
		{"Rue de la Kasba 7", "Tunis", "1006", "11", "TN", ""},
		{"Avenue de France 22", "Tunis", "1000", "11", "TN", ""},
	},
	"TO": {
		{"Taufa'ahau Rd 14", "Nuku'alofa", "", "04", "TO", ""},
		{"Salote Rd 7", "Nuku'alofa", "", "04", "TO", ""},
		{"Vuna Rd 3", "Nuku'alofa", "", "04", "TO", ""},
	},
	"TR": {
		{"Istiklal Cad. 14", "Istanbul", "34433", "34", "TR", ""},
		{"Ataturk Blvd 7", "Ankara", "06050", "06", "TR", ""},
		{"Cumhuriyet Blvd 22", "Izmir", "35210", "35", "TR", ""},
	},
	"TT": {
		{"Independence Square 14", "Port of Spain", "", "POS", "TT", ""},
		{"Frederick St 7", "Port of Spain", "", "POS", "TT", ""},
		{"Ariapita Ave 22", "Port of Spain", "", "POS", "TT", ""},
	},
	"TV": {
		{"Funafuti Rd 5", "Funafuti", "", "FUN", "TV", ""},
		{"Vaiaku Rd 12", "Funafuti", "", "FUN", "TV", ""},
		{"Teone Rd 3", "Funafuti", "", "FUN", "TV", ""},
	},
	"TW": {
		{"Zhongxiao East Rd 14", "Taipei", "100", "TPE", "TW", ""},
		{"Xinyi Rd 7", "Taipei", "110", "TPE", "TW", ""},
		{"Zhongshan N Rd 22", "Taipei", "104", "TPE", "TW", ""},
	},
	"TZ": {
		{"Samora Ave 14", "Dodoma", "40487", "03", "TZ", ""},
		{"Nyerere Rd 7", "Dar es Salaam", "11101", "02", "TZ", ""},
		{"Makunganya St 22", "Dar es Salaam", "11101", "02", "TZ", ""},
	},
	"UA": {
		{"Khreshchatyk St 14", "Kyiv", "01001", "30", "UA", ""},
		{"Shevchenko Blvd 7", "Kyiv", "01033", "30", "UA", ""},
		{"Sumska St 22", "Kharkiv", "61002", "63", "UA", ""},
	},
	"UG": {
		{"Kampala Rd 14", "Kampala", "", "101", "UG", ""},
		{"Entebbe Rd 7", "Kampala", "", "101", "UG", ""},
		{"Ben Kiwanuka St 22", "Kampala", "", "101", "UG", ""},
	},
	"US": {
		{"142 W 57th St", "New York", "10019", "NY", "US", ""},
		{"831 Beacon St", "Boston", "02215", "MA", "US", ""},
		{"2118 Wilshire Blvd", "Los Angeles", "90403", "CA", "US", ""},
	},
	"UY": {
		{"18 de Julio Ave 14", "Montevideo", "11100", "MO", "UY", ""},
		{"Av. Brasil 7", "Montevideo", "11300", "MO", "UY", ""},
		{"Reconquista 22", "Montevideo", "11000", "MO", "UY", ""},
	},
	"UZ": {
		{"Amir Temur St 14", "Tashkent", "100000", "TK", "UZ", ""},
		{"Navoi Ave 7", "Tashkent", "100011", "TK", "UZ", ""},
		{"Mustakillik Ave 22", "Tashkent", "100029", "TK", "UZ", ""},
	},
	"VC": {
		{"Bay St 14", "Kingstown", "VC0100", "06", "VC", ""},
		{"Grenville St 7", "Kingstown", "VC0100", "06", "VC", ""},
		{"Halifax St 3", "Kingstown", "VC0100", "06", "VC", ""},
	},
	"VE": {
		{"Av. Libertador 14", "Caracas", "1050", "A", "VE", ""},
		{"Av. Francisco de Miranda 7", "Caracas", "1060", "A", "VE", ""},
		{"Calle Real de Sabana Gr 3", "Caracas", "1042", "A", "VE", ""},
	},
	"VN": {
		{"Trang Tien St 14", "Hanoi", "100000", "HN", "VN", ""},
		{"Dong Khoi St 7", "Ho Chi Minh", "700000", "SG", "VN", ""},
		{"Le Loi Blvd 22", "Ho Chi Minh", "700000", "SG", "VN", ""},
	},
	"VU": {
		{"Kumul Hwy 14", "Port Vila", "", "SEE", "VU", ""},
		{"Lini Hwy 7", "Port Vila", "", "SEE", "VU", ""},
		{"Rue Bougainville 3", "Port Vila", "", "SEE", "VU", ""},
	},
	"WS": {
		{"Beach Rd 14", "Apia", "", "TU", "WS", ""},
		{"Ififi Rd 7", "Apia", "", "TU", "WS", ""},
		{"Falealili St 3", "Apia", "", "TU", "WS", ""},
	},
	"XK": {
		{"Bill Clinton Blvd 14", "Pristina", "10000", "PR", "XK", ""},
		{"Nëna Terezë St 7", "Pristina", "10000", "PR", "XK", ""},
		{"Agim Ramadani St 3", "Pristina", "10000", "PR", "XK", ""},
	},
	"YE": {
		{"Hadda St 14", "Sanaa", "", "SA", "YE", ""},
		{"60 Meter Rd 7", "Sanaa", "", "SA", "YE", ""},
		{"Zubairi St 22", "Sanaa", "", "SA", "YE", ""},
	},
	"ZA": {
		{"Bree St 14", "Cape Town", "8001", "WC", "ZA", ""},
		{"Jan Smuts Ave 7", "Johannesburg", "2196", "GP", "ZA", ""},
		{"Francis Baard St 22", "Pretoria", "0002", "GT", "ZA", ""},
	},
	"ZM": {
		{"Cairo Rd 14", "Lusaka", "10101", "09", "ZM", ""},
		{"Independence Ave 7", "Lusaka", "10101", "09", "ZM", ""},
		{"Great East Rd 22", "Lusaka", "10101", "09", "ZM", ""},
	},
	"ZW": {
		{"Samora Machel Ave 14", "Harare", "", "HA", "ZW", ""},
		{"Jason Moyo Ave 7", "Harare", "", "HA", "ZW", ""},
		{"Rotten Row 22", "Harare", "", "HA", "ZW", ""},
	},
}

// addrRot drives row rotation; atomic so concurrent checks are safe.
var addrRot uint64

// addrFor returns the next rotating row for a country code. Falls back
// to the legacy single-row Book, then signals a miss (callers then use
// Book["DEFAULT"]).
func addrFor(cc string) (Address, bool) {
	if rows, ok := BookMulti[cc]; ok && len(rows) > 0 {
		i := atomic.AddUint64(&addrRot, 1) - 1
		return rows[int(i%uint64(len(rows)))], true
	}
	if a, ok := Book[cc]; ok {
		return a, true
	}
	return Address{}, false
}

// fullDial: E.164 dial codes for every researched country. Fills gaps in
// dialCodes at init so randomizedPhones produces a number for ALL 196
// book countries (legacy dialCodes only covered ~43).
var fullDial = map[string]string{
	"AD": "+376",
	"AE": "+971",
	"AF": "+93",
	"AG": "+1",
	"AI": "+1",
	"AL": "+355",
	"AM": "+374",
	"AO": "+244",
	"AR": "+54",
	"AT": "+43",
	"AU": "+61",
	"AW": "+297",
	"AZ": "+994",
	"BA": "+387",
	"BB": "+1",
	"BD": "+880",
	"BE": "+32",
	"BF": "+226",
	"BG": "+359",
	"BH": "+973",
	"BI": "+257",
	"BJ": "+229",
	"BM": "+1",
	"BN": "+673",
	"BO": "+591",
	"BR": "+55",
	"BS": "+1",
	"BT": "+975",
	"BW": "+267",
	"BY": "+375",
	"BZ": "+501",
	"CA": "+1",
	"CD": "+243",
	"CF": "+236",
	"CG": "+242",
	"CH": "+41",
	"CK": "+682",
	"CL": "+56",
	"CM": "+237",
	"CN": "+86",
	"CO": "+57",
	"CR": "+506",
	"CU": "+53",
	"CV": "+238",
	"CW": "+599",
	"CY": "+357",
	"CZ": "+420",
	"DE": "+49",
	"DJ": "+253",
	"DK": "+45",
	"DM": "+1",
	"DO": "+1",
	"DZ": "+213",
	"EC": "+593",
	"EE": "+372",
	"EG": "+20",
	"ER": "+291",
	"ES": "+34",
	"ET": "+251",
	"FI": "+358",
	"FJ": "+679",
	"FM": "+691",
	"FO": "+298",
	"FR": "+33",
	"GA": "+241",
	"GB": "+44",
	"GD": "+1",
	"GE": "+995",
	"GF": "+594",
	"GH": "+233",
	"GI": "+350",
	"GL": "+299",
	"GM": "+220",
	"GN": "+224",
	"GP": "+590",
	"GQ": "+240",
	"GR": "+30",
	"GT": "+502",
	"GU": "+1",
	"GW": "+245",
	"GY": "+592",
	"HK": "+852",
	"HN": "+504",
	"HR": "+385",
	"HT": "+509",
	"HU": "+36",
	"ID": "+62",
	"IE": "+353",
	"IL": "+972",
	"IN": "+91",
	"IQ": "+964",
	"IR": "+98",
	"IS": "+354",
	"IT": "+39",
	"JM": "+1",
	"JO": "+962",
	"JP": "+81",
	"KE": "+254",
	"KG": "+996",
	"KH": "+855",
	"KI": "+686",
	"KM": "+269",
	"KN": "+1",
	"KP": "+850",
	"KR": "+82",
	"KW": "+965",
	"KY": "+1",
	"KZ": "+7",
	"LA": "+856",
	"LB": "+961",
	"LC": "+1",
	"LI": "+423",
	"LK": "+94",
	"LR": "+231",
	"LS": "+266",
	"LT": "+370",
	"LU": "+352",
	"LV": "+371",
	"LY": "+218",
	"MA": "+212",
	"MC": "+377",
	"MD": "+373",
	"ME": "+382",
	"MG": "+261",
	"MH": "+692",
	"MK": "+389",
	"ML": "+223",
	"MM": "+95",
	"MN": "+976",
	"MO": "+853",
	"MQ": "+596",
	"MR": "+222",
	"MS": "+1",
	"MT": "+356",
	"MU": "+230",
	"MV": "+960",
	"MW": "+265",
	"MX": "+52",
	"MY": "+60",
	"MZ": "+258",
	"NA": "+264",
	"NC": "+687",
	"NE": "+227",
	"NG": "+234",
	"NI": "+505",
	"NL": "+31",
	"NO": "+47",
	"NP": "+977",
	"NR": "+674",
	"NU": "+683",
	"NZ": "+64",
	"OM": "+968",
	"PA": "+507",
	"PE": "+51",
	"PF": "+689",
	"PG": "+675",
	"PH": "+63",
	"PK": "+92",
	"PL": "+48",
	"PM": "+508",
	"PS": "+970",
	"PT": "+351",
	"PW": "+680",
	"PY": "+595",
	"QA": "+974",
	"RE": "+262",
	"RO": "+40",
	"RS": "+381",
	"RU": "+7",
	"RW": "+250",
	"SA": "+966",
	"SB": "+677",
	"SC": "+248",
	"SD": "+249",
	"SE": "+46",
	"SG": "+65",
	"SH": "+290",
	"SI": "+386",
	"SK": "+421",
	"SL": "+232",
	"SM": "+378",
	"SN": "+221",
	"SO": "+252",
	"SR": "+597",
	"SS": "+211",
	"ST": "+239",
	"SV": "+503",
	"SY": "+963",
	"SZ": "+268",
	"TC": "+1",
	"TD": "+235",
	"TG": "+228",
	"TH": "+66",
	"TJ": "+992",
	"TK": "+690",
	"TL": "+670",
	"TM": "+993",
	"TN": "+216",
	"TO": "+676",
	"TR": "+90",
	"TT": "+1",
	"TV": "+688",
	"TW": "+886",
	"TZ": "+255",
	"UA": "+380",
	"UG": "+256",
	"US": "+1",
	"UY": "+598",
	"UZ": "+998",
	"VA": "+39",
	"VC": "+1",
	"VE": "+58",
	"VN": "+84",
	"VU": "+678",
	"WF": "+681",
	"WS": "+685",
	"XK": "+383",
	"YE": "+967",
	"YT": "+262",
	"ZA": "+27",
	"ZM": "+260",
	"ZW": "+263",
	"CI": "+225", // Cote d'Ivoire: absent from prefixes2.json
}

func init() {
	for cc, d := range fullDial {
		if _, ok := dialCodes[cc]; !ok {
			dialCodes[cc] = strings.TrimPrefix(d, "+")
		}
	}
	// CI is the one book country absent from the v14 phone research.
	if _, ok := nationalLengths["CI"]; !ok {
		nationalLengths["CI"] = 10
	}
	if _, ok := mobilePrefixList["CI"]; !ok {
		mobilePrefixList["CI"] = []string{"01", "05", "07", "25", "27", "45", "47", "55", "57", "65", "67", "75", "77", "85", "87", "95", "97"}
	}
}
