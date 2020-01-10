import requests
import datetime as dt
import pandas as pd
from tqdm import tqdm

'''
TO DO:
- Some kind of mapping feature.
- Storage options: Big Query, etc. 
- Alerts.
- Stream data to storage; constant updates.
'''

class Client:

    def __init__(self):
        self.base = "http://skiplagged.com"
        self.days = 90

    def search(self, from_, to_=None, depart_=None, return_=None):
        """
        from_ (required)| departure airport(s) | list of airport codes (3 letters)
        to_ (optional)| arrival airport(s) | list of airport codes (3 letters). Default is all airports. 
        depart_ (optional) | departure date | list of dt.dates. Default is every day in next six months. 
        return_ (optional) | return date | list of dt.dates. Default is no return flights. 
        """

        if not isinstance(from_, list):
            raise ValueError("Departure airport must be a list.")

        if to_:
            if not isinstance(to_, list):
                raise ValueError("Arrival airport must be a list.")
        else:
            to_ = pd.read_csv("data/airports.csv")["IATA"].tolist()

        if depart_:
            if not isinstance(depart_, list):
                raise ValueError("Departure must be list of dt.dates.")
        else:
            base = dt.datetime.today()
            depart_ = [base + dt.timedelta(days=x) for x in range(self.days)]

        if return_:
            if not isinstance(depart_, list):
                raise ValueError("Return must be a list of dt.dates.")
        
        if depart_ and return_:
            if len(depart_) != len(return_):
                raise ValueError("If providing departure and return dates, the lists must be the same size.")


        kwargs = {"from_" : from_, "depart_" : depart_, "to_" : to_, "return_" : return_}

        params = self.generate_flight_parms(**kwargs)

        dfs = []
        for param in tqdm(params):
            data = self.api(**param)
            df = self.json_to_df(data)
            dfs.append(df)

        return pd.concat(dfs)

    def generate_flight_parms(self, from_, to_, depart_, return_):

        parms = []
        for dep_air in from_: 
            for arr_air in to_:
                if dep_air != arr_air and str(arr_air) != "nan":
                    for date in depart_:
                        parms.append({
                            "from_" : dep_air, 
                            "to_" : arr_air,
                            "depart_" : dt.datetime.strftime(date, "%Y-%m-%d"),
                            "return_" : dt.datetime.strftime(return_, "%Y-%m-%d") if return_ else return_
                        })

        return parms

    def api(self, from_, to_, depart_, return_):

        url = self.base + "/api/search.php?"
        url += "from={from_}&to={to_}&depart={depart_}".format(from_=from_, to_=to_, depart_=depart_)
        if return_:
            url += "&return={return_}".format(return_=dt.datetime.strftime(return_, "%Y-%m-%d"))

        response = requests.get(url)
        
        return response.json()

    def json_to_df(self, data):
        output = []
        if len(data.get("flights")) != 0:
            for key, values in data.get("flights").items():
                trip_id = key
                legs = values[0]
                total_legs = len(legs)
                for leg_num in range(0, total_legs):
                    flight_id = legs[leg_num][0]
                    dep_airport = legs[leg_num][1]
                    dep_dt = legs[leg_num][2]
                    arr_airport = legs[leg_num][3]
                    arr_dt = legs[leg_num][4]
                    row = [trip_id, flight_id, dep_airport, dep_dt, arr_airport, arr_dt, leg_num + 1, total_legs, values[1]]
                    output.append(row)

        return pd.DataFrame(output, columns=["trip_id", "flight_id", "dep_airport", "dep_dt",
        "arr_airport", "arr_dt", "leg_num", "total_legs", "price"])

if __name__=="__main__":
    data = Client().search(from_=["DFW"], to_=["SAN"])
    print(data)
    

    
        