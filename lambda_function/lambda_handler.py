import json
from inventory_forecaster.forecaster import stockage_dict, FORECAST_DAYS, buffer_days


def lambda_handler(event, context):
    try:
        print('TEST')
        print('Forecast Days: ', FORECAST_DAYS)
        print('Buffer Days: ', buffer_days)
        print(stockage_dict)


        return json.dumps(stockage_dict) if stockage_dict else json.dumps([])
    except Exception as e:
        return str(e)