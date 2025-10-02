from vol_dashboard.connector.db_connector import VolDbConnector


if __name__ == "__main__":
    db_conn = VolDbConnector()
    db_conn.insert_events(ECONOMIC_EVENTS)
    ret = db_conn.get_events()
    for event, date, time in ret:
        print(f"{event}: {date} at {time} ET")
