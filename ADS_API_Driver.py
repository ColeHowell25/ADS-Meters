#Author: Cole Howell
#Purpose: Run the ads api to acquire data to input into gis
#Completed: 1/23/2023

from ADS_API_functions import *


def main():

    #configure functions with sensitive information
    g, a, h = config()

    #define the logbook location
    book = log_book_location()

    #access the ADS data
    active = get_active_meters(a)
    ids = get_meter_ids(active)
    names = get_meter_names(active)
    telemetries = get_active_telemetry(ids, a)

    #calculate the daily total flow for each meter
    tritons = calculate_daily_totals(telemetries, names)

    #log the data in the hosted table on portal
    date = get_yesterday()
    log_in_gis(telemetries, tritons, g)
    gis_mass_balance(date, g, h)

    #log data in the flow logbook
    log_totals(tritons, book)
    log_rain(telemetries, book)
    format_logbook(book)

    #update the sensors for the daily flow
    update_gis(tritons, g)

    #calculations for the I/I map
    # average = running_average(book)
    # rain = last_rain(book)
    # mean_rain = avg_rain(book)
    # u_test = mann_whitney_u(book)
    # infiltration_zones(average, mean_rain, rain, u_test)
    # last_rain_balance(book)

    # histogram_generator(book)

    # print(u_test)


if __name__ == "__main__":
    main()
