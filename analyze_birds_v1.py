import os
from molgenis_emx2_pyclient import Client
from dotenv import load_dotenv

load_dotenv()
token = os.environ.get("MOLGENIS_TOKEN")


def create_mol_query(querylist):

    # accepts a list of queries and creates a query filter string for the molgens pyclient
    # queries are lists: [logic combinator, column, operator, value]
    # logic combinator = and / or / not
    qstring =""
    for each in querylist:
        qstring += f'{each[0]}{each[1]}{each[2]}{each[3]}'

    return qstring

wvogel = [['','confidence','>=','0.7'],

            [' and ', 'speciesCode', '==', 'norlap'], # Kievit
            [' and ', 'speciesCode', '==', 'pieavo1']]  # Kluut

# [' and ', 'speciesCode', '==', 'meapip1'],  # Graspieper
# [' and ', 'speciesCode', '==', 'bktgod'],  # Grutto
# [' and ', 'speciesCode', '==', 'comred1'],  # Tureluur
# [' and ', 'speciesCode', '==', 'skylar'],  # Veldleeuwerik
# [' and ', 'speciesCode', '==', 'whimbr']]  # Wulp


#["",'confidence','>=','0.7']
#print(create_mol_query(wvogel))

with Client(url='https://molgenis.paraiko.com', token=token, schema='vhl_wvp_sound_1') as client:

    # Perform tasks
    #print(client.schema_names)
    #print(client.get_schema_metadata)
    speciescode = 'norlap'
    confidence = 0.7
    data = client.get(table='wvp_1',
                            columns=['id', 'CommonName', 'SpeciesCode', 'Confidence', 'Ronde',
                                                    'location','songmeter','filename','date', 'NewTime'],
                            #query_filter=f'{create_mol_query(wvogel)}',       #confidence >= 0.98'
                            #f' and location==Lollum',
                            #query_filter=f'speciesCode=={speciescode} and confidence>={confidence}',
                            query_filter=f'confidence>={confidence}',
                            as_df=True)

    print(data.head())
    print(data.info())

    # Replacing 'songmeter'  19 with 21 (songmeter is vervangen))
    print(data)
    data.loc[data["songmeter"] == "Songmeter019", 'songmeter'] = "Songmeter021"
    print(data)
    gb_songm = data.groupby(['date','SpeciesCode','songmeter',]).agg(
        SpeciesCode=('id', 'count')
    )
    print(gb_songm)
    gb_songm.to_csv('output/gb_songm2.csv')


    # filtered_df = table_data[(table_data['Confidence'] >= 0.7) &
    #                          (table_data['SpeciesCode'] == 'meapip1') |
    #                          (table_data['SpeciesCode'] == 'norlap')]
    #
    # print(filtered_df.info())

