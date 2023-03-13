import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import locale
import pandas as pd
import sqlite3 as sl

today = datetime.now()
date_extract = (today + timedelta(1)).strftime('%Y-%m-%d')
# print(date_extract)

locale.setlocale(locale.LC_ALL, 'fr_FR')
nb_places_defaut = 3

# Create an URL object
page = 0
url_base = 'https://www.amigoexpress.com'
url_page = '/covoiturages/de-montreal/qc?date=' + date_extract + '&p='
url = url_base + url_page
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

with requests.Session() as s:
    s.headers = headers
    liste_row = []
    while True:
        page = page + 1
        # Create object page
        url = url_base + url_page + str(page)
        page_content = requests.get(url, headers=headers)

        soup = BeautifulSoup(page_content.text, 'lxml')
        # Obtain information from tag <table>
        table1 = soup.find_all('table', id='rideTable')

        # Define Date
        currentDateTime = datetime.now()
        year = currentDateTime.date().strftime("%Y")
        date_row = table1[1].find_all('tr')[0].find_all('td')[0]
        date_string = date_row.text  + ' ' + year
        date = datetime.strptime(date_string, '%A %d %B %Y').strftime('%Y-%m-%d')

        for j in table1[1].find_all('tr')[1:]:
            # print('-------------------------NEW LINE-------------------------\n')
            row_data = j.find_all('td')
            row = []
            # print('date', date)
            row.append(date)
            for column in row_data:
                # print('-------------------------NEW COLUMNS-------------------------\n',column)
                # TIME
                if column.has_attr('class'):
                    if column['class'][0] == 'datetime':
                        time = column.get_text().strip()
                        # print('time: ', time)
                        row.append(time)
                # DEPART
                city_depart = column.find('div', class_='city departure')
                if city_depart:
                    depart_ville_div = city_depart.find('strong')
                    if depart_ville_div:
                        depart_ville = depart_ville_div.get_text()
                        # print('depart_ville: ', depart_ville)
                        row.append(depart_ville)
                    depart_lieu_div = city_depart.find('span', class_='pickupDetails')
                    if depart_lieu_div:
                        depart_lieu = depart_lieu_div.get_text()
                        # print('depart_lieu: ', depart_lieu)
                        row.append(depart_lieu)
                # DESTINATION
                city_destination = column.find('div', class_='city destination')
                if city_destination:
                    destination_ville_div = city_destination.find('strong')
                    if destination_ville_div:
                        destination_ville = destination_ville_div.get_text()
                        # print('destination_ville: ', destination_ville)
                        row.append(destination_ville)
                    destination_lieu_div = city_destination.find('span', class_='pickupDetails')
                    if destination_lieu_div:
                        destination_lieu = destination_lieu_div.get_text()
                        # print('destination_lieu: ', destination_lieu)
                        row.append(destination_lieu)
                # TARIF & PLACES
                if column.has_attr('class'):
                    if column['class'][0] == 'seatsAvailability':
                        itineraryPrice_div  = column.find('div', class_='itineraryPrice')
                # TARIF
                        tarif_div  = itineraryPrice_div.find('a', title='Prix du conducteur')
                        if tarif_div : 
                            tarif = tarif_div.get_text()
                            # print('tarif: ', tarif)
                            tarif_int = locale.atof(tarif.strip("$"))
                            row.append(tarif_int)
                        else:
                            row.append(0)
                # PLACES
                        places_offertes  = itineraryPrice_div.findAll('img', class_='blueMan')
                        if places_offertes:
                            tarif = tarif_div.get_text()
                            # NOMBRE DE PLACES OFFERTES
                            # print('places_offertes: ', len(places_offertes))
                            row.append(len(places_offertes))
                            # NOMBRE DE PLACES DISPO
                            places_dispo = []
                            places_dispo  = itineraryPrice_div.findAll('img', alt='White Man')
                            # print('places_dispo: ', len(places_dispo))
                            row.append(len(places_dispo))
                            # NOMBRE DE PLACES RESERVEES
                            places_reservees= []
                            places_reservees  = itineraryPrice_div.findAll('img', alt='Blue Man')
                            # print('places_reservees: ', len(places_reservees))
                            row.append(len(places_reservees))
                            #COMPLET = FALSE
                            # print('COMPLET: ', False)
                            row.append(False)
                        else:
                            complet  = itineraryPrice_div.findAll('img', title='Aucune place disponible')
                            # ON INDIQUE UN NOMBRE DE PLACE OFFERTE PAR DEFAUT
                            # print('places_offertes: ', nb_places_defaut)
                            row.append(nb_places_defaut)
                            # NOMBRE DE PLACES DISPO
                            # print('places_dispo: ', 0)
                            row.append(0)
                            # NOMBRE DE PLACES RESERVEES = NOMBRE DE PLACE OFFERTE PAR DEFAUT
                            # print('places_reservees: ', nb_places_defaut)
                            row.append(nb_places_defaut)
                            #COMPLET = TRUE
                            # print('COMPLET: ', True)
                            row.append(True)


            # print('nb de colonne', len(row))
            liste_row.append(row)
            
        next_page = soup.find('a', class_ = "Next")
        if next_page is None:
            break

        # if page == 1:
        #     break


columns = ['depart_date', 'depart_heure', 'Depart_ville','Depart_lieu', 'Arrivee_ville', 'Arrivee_lieu','tarif ($)', 'nb_places_offertes','nb_places_dispo', 'nb_places_reservees', 'complet']
covoiturage_df = pd.DataFrame(liste_row, columns= columns)
covoiturage_df.info()

con = sl.connect('covoiturage_from_montreal.sqlite')
with con:
    covoiturage_df.to_sql('Historique_Offres_Brutes', con, if_exists='append', index=False)