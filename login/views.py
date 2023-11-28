from datetime import datetime
import logging
from sqlite3 import Cursor
from django.http import JsonResponse
from rest_framework.views import APIView
import pyodbc
from rest_framework.response import Response


logger = logging.getLogger(__name__)
def database():
    NAME = 'NewColumbusTawfeeq'
    HOST = 'DESKTOP-21ATSCV'
    USER = 'sa'
    PASSWORD = 'sa@123'
    connection_string = f'DRIVER=SQL Server; SERVER={HOST};DATABASE={NAME};UID={USER};PWD={PASSWORD};'
    conn = pyodbc.connect(connection_string)
    return conn

class verify_user(APIView):
    logger = logging.getLogger(__name__)

    def post(self, request, format=None):
        try:
            conn = database()
            cursor = conn.cursor()
            cursor.execute("EXEC CheckUserLogin_TO ?, ?", (request.data['username'], request.data['password']))
            user = cursor.fetchone()
            #print(user)

            if user is not None:
                if user[2] == 1:
                    response_data = {'RESPONSE': 1, 'DATA': [{'DEPTCODE': user[0], 'Email': user[1]}], 'ERROR': ''}
                    self.logger.info(f"Successful login attempt for username '{request.data['username']}': {response_data}")
                elif user[2] == 0:
                    response_data = {'RESPONSE': 0, 'DATA': [], 'ERROR': 'USER IS INACTIVE'}
                    self.logger.warning(f"Failed login attempt for inactive username '{request.data['username']}': {response_data}")
            else:
                response_data = {'RESPONSE': 0, 'DATA': [], 'ERROR': f"{request.data['username']} is Invalid"}
                self.logger.warning(f"Failed login attempt for invalid username '{request.data['username']}': {response_data}")

            cursor.close()
            conn.close()
            return JsonResponse(response_data, safe=False)
        except Exception as e:
            error_message = str(e)
            response_data = {'RESPONSE': 0, 'DATA': [], 'ERROR': error_message}
            self.logger.error(f"Error during login attempt for username '{request.data['username']}': {error_message}")
            return JsonResponse(response_data, safe=False)




'''
class booking(APIView):
    logger = logging.getLogger(__name__)
    def get(self, request, format=None):
        conn = database() 
        cursor = conn.cursor()
        transferdate = request.data.get('transferdate', None)
        sortby = request.data.get('sortby', None)
        RId = request.data.get('RId', None)
        try:
            
            cursor.execute("exec [dbo].[sp_assigntransfers_get_service_details] ?, ?, ?", (transferdate, sortby, RId))
            results = cursor.fetchall()
            print(results)
            Response_data = [dict(zip([column[0] for column in cursor.description], row)) for row in results]
            Response_data = {'Response':0, 'Data':Response_data, 'Error':""}
            self.logger.info(f"Successful Data Fetched {request.data['transferdate']} - {Response_data}")

            
            cursor.close()
            conn.close()
            return JsonResponse(Response_data, safe= False)
        except Exception as e:
            error_message = str(e)
            Response_data= {'RESPONSE': 1, 'DATA': [], 'ERROR': error_message}
            self.logger.warning(f"Error During Data Fetched {request.data['transferdate']} - {Response_data}")
            return JsonResponse(Response_data,safe= False)
 '''
'''
class filter(APIView):
    def get(self, request, format=None):
        try:
            conn = database()
            cursor = conn.cursor()
            requestid = request.data.get("requestid")
            tlineno = request.data.get("tlineno")
            transfertype = request.data.get("transfertype")
            airportbordercode = request.data.get("airportbordercode")
            sectorgroupcode = request.data.get("sectorgroupcode")
            cartypecode = request.data.get("cartypecode")
            shuttle = request.data.get("shuttle")
            transferdate = request.data.get("transferdate")
            flightcode = request.data.get("flightcode")
            flight_tranid = request.data.get("flight_tranid")
            flighttime = request.data.get("flighttime")
            pickup = request.data.get("pickup")
            dropoff = request.data.get("dropoff")
            adults = request.data.get("adults")
            child = request.data.get("child")
            childagestring = request.data.get("childagestring")
            units = request.data.get("units")
            unitprice = request.data.get("unitprice")
            unitsalevalue = request.data.get("unitsalevalue")
            tplistcode = request.data.get("tplistcode")
            complimentarycust = request.data.get("complimentarycust")
            wlunitprice = request.data.get("wlunitprice")
            wlunitsalevalue = request.data.get("wlunitsalevalue")
            updatteddate = request.data.get("updatteddate")
            updateduser = request.data.get("updateduser")
            overrideprice = request.data.get("overrideprice")
            flightclass = request.data.get("flightclass")
            preferredsupplier = request.data.get("preferredsupplier")
            unitcprice = request.data.get("unitcprice")
            unitcostvalue = request.data.get("unitcostvalue")
            tcplistcode = request.data.get("tcplistcode")
            wlcurrcode = request.data.get("wlcurrcode")
            wlconvrate = request.data.get("wlconvrate")
            wlmarkupperc = request.data.get("wlmarkupperc")
            CostTaxableValue = request.data.get("CostTaxableValue")
            CostVATValue = request.data.get("CostVATValue")
            VATPer = request.data.get("VATPer")
            PriceWithTAX = request.data.get("PriceWithTAX")
            PriceTaxableValue = request.data.get("PriceTaxableValue")
            PriceVATValue = request.data.get("PriceVATValue")
            PriceVATPer = request.data.get("PriceVATPer")
            PriceWithTAX1 = request.data.get("PriceWithTAX1")
            BookingMode = request.data.get("BookingMode")
            Pickupcodetype = request.data.get("Pickupcodetype")
            Dropoffcodetype = request.data.get("Dropoffcodetype")
            cursor.execute("exec Booking_Filters_TO ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? ", requestid,tlineno,transfertype,airportbordercode, sectorgroupcode,cartypecode,shuttle,transferdate,flightcode,flight_tranid,flighttime,pickup,dropoff,adults,child,childagestring,units,unitprice,unitsalevalue, tplistcode,complimentarycust,wlunitprice,wlunitsalevalue,updatteddate,updateduser,overrideprice,flightclass,preferredsupplier,unitcprice,unitcostvalue,tcplistcode,wlcurrcode,wlconvrate,wlmarkupperc,CostTaxableValue,CostVATValue,VATPer,PriceWithTAX,PriceTaxableValue,PriceVATValue,PriceVATPer,PriceWithTAX1,BookingMode,Pickupcodetype,Dropoffcodetype)
            results = cursor.fetchall()
            Response_data = [dict(zip([column[0] for column in cursor.description], row)) for row in results]
            Response_data = {'Response':0, 'Data':Response_data, 'Error':""}


            cursor.close()
            conn.close()
            return JsonResponse(Response_data, safe= False)
        except Exception as e:
            return JsonResponse({'error': str(e)})'''
class bookings(APIView):
    logger = logging.getLogger(__name__)

    def get(self, request, format=None):
        conn = database() 
        cursor = conn.cursor()
        transferdate = request.data.get('transferdate', None)
        sortby = request.data.get('sortby', None)
        RId = request.data.get('RId', None)
        
        try:
            cursor.execute("exec [dbo].[sp_assigntransfers_get_service_details] ?, ?, ?", (transferdate, sortby, RId))
            results = cursor.fetchall()
            
            filtered_results = self.apply_filters(results, request.data, cursor)
            response_data = {'Response': 1, 'Data': filtered_results, 'Error': ""}
            self.logger.info(f"Successful Data Fetched {request.data['transferdate']} - {response_data}")

            cursor.close()
            conn.close()
            return JsonResponse(response_data, safe=False)
        except Exception as e:
            error_message = str(e)
            response_data = {'RESPONSE': 0, 'DATA': [], 'ERROR': error_message}
            self.logger.warning(f"Error During Data Fetched {request.data['transferdate']} - {response_data}")
            return JsonResponse(response_data, safe=False)

    def apply_filters(self, results, filters, cursor):
        transfertype_filter = filters.get('transfertype')
        service_type_filter = filters.get('ServiceType')
        requestid = filters.get('requestid')
        tlineno = filters.get('tlineno')
        airportbordercode = filters.get('airportbordercode')
        sectorgroupcode = filters.get('sectorgroupcode')
        cartypecode = filters.get('cartypecode')
        shuttle = filters.get('shuttle')
        flightcode = filters.get('flightcode')
        flight_tranid = filters.get('flight_tranid')
        flighttime = filters.get('flighttime')
        pickup = filters.get('pickup')
        dropoff = filters.get('dropoff')
        adults = filters.get('adults')
        child = filters.get('child')
        childagestring = filters.get('childagestring')
        units = filters.get('units')
        unitprice = filters.get('unitprice')
        unitsalevalue = filters.get('unitsalevalue')
        tplistcode = filters.get('tplistcode')
        complimentarycust = filters.get('complimentarycust')
        wlunitprice = filters.get('wlunitprice')
        wlunitsalevalue = filters.get('wlunitsalevalue')
        updatteddate = filters.get('updatteddate')
        updateduser = filters.get('updateduser')
        overrideprice = filters.get('overrideprice')
        flightclass = filters.get('flightclass')
        preferredsupplier = filters.get('preferredsupplier')
        unitcprice = filters.get('unitcprice')
        unitcostvalue = filters.get('unitcostvalue')
        tcplistcode = filters.get('tcplistcode')
        wlcurrcode = filters.get('wlcurrcode')
        wlconvrate = filters.get('wlconvrate')
        wlmarkupperc = filters.get('wlmarkupperc')
        CostTaxableValue = filters.get('CostTaxableValue')
        CostVATValue = filters.get('CostVATValue')
        VATPer = filters.get('VATPer')
        PriceWithTAX = filters.get('PriceWithTAX')
        PriceTaxableValue = filters.get('PriceTaxableValue')
        PriceVATValue = filters.get('PriceVATValue')
        PriceVATPer = filters.get('PriceVATPer')
        PriceWithTAX1 = filters.get('PriceWithTAX1')
        BookingMode = filters.get('BookingMode')
        Pickupcodetype = filters.get('Pickupcodetype')
        Dropoffcodetype = filters.get('Dropoffcodetype')


        filtered_results = []

        for row in results:
            row_dict = dict(zip([column[0] for column in cursor.description], row))
            if (not transfertype_filter or row_dict['transfertype'] == transfertype_filter) and \
                    (not service_type_filter or row_dict['ServiceType'] == service_type_filter) and \
                    (not requestid or row_dict['requestid'] == requestid) and \
                    (not tlineno or row_dict['tlineno'] == tlineno) and \
                    (not airportbordercode or row_dict['airportbordercode'] == airportbordercode) and \
                    (not sectorgroupcode or row_dict['sectorgroupcode'] == sectorgroupcode) and \
                    (not cartypecode or row_dict['cartypecode'] == cartypecode) and \
                    (not shuttle or row_dict['shuttle'] == shuttle) and \
                    (not flightcode or row_dict['flightcode'] == flightcode) and \
                    (not flight_tranid or row_dict['flight_tranid'] == flight_tranid) and \
                    (not flighttime or row_dict['flighttime'] == flighttime) and \
                    (not pickup or row_dict['pickup'] == pickup) and \
                    (not dropoff or row_dict['dropoff'] == dropoff) and \
                    (not adults or row_dict['adults'] == adults) and \
                    (not child or row_dict['child'] == child) and \
                    (not childagestring or row_dict['childagestring'] == childagestring) and \
                    (not units or row_dict['units'] == units) and \
                    (not unitprice or row_dict['unitprice'] == unitprice) and \
                    (not unitsalevalue or row_dict['unitsalevalue'] == unitsalevalue) and \
                    (not tplistcode or row_dict['tplistcode'] == tplistcode) and \
                    (not complimentarycust or row_dict['complimentarycust'] == complimentarycust) and \
                    (not wlunitprice or row_dict['wlunitprice'] == wlunitprice) and \
                    (not wlunitsalevalue or row_dict['wlunitsalevalue'] == wlunitsalevalue) and \
                    (not updatteddate or row_dict['updatteddate'] == updatteddate) and \
                    (not updateduser or row_dict['updateduser'] == updateduser) and \
                    (not overrideprice or row_dict['overrideprice'] == overrideprice) and \
                    (not flightclass or row_dict['flightclass'] == flightclass) and \
                    (not preferredsupplier or row_dict['preferredsupplier'] == preferredsupplier) and \
                    (not unitcprice or row_dict['unitcprice'] == unitcprice) and \
                    (not unitcostvalue or row_dict['unitcostvalue'] == unitcostvalue) and \
                    (not tcplistcode or row_dict['tcplistcode'] == tcplistcode) and \
                    (not wlcurrcode or row_dict['wlcurrcode'] == wlcurrcode) and \
                    (not wlconvrate or row_dict['wlconvrate'] == wlconvrate) and \
                    (not wlmarkupperc or row_dict['wlmarkupperc'] == wlmarkupperc) and \
                    (not CostTaxableValue or row_dict['CostTaxableValue'] == CostTaxableValue) and \
                    (not CostVATValue or row_dict['CostVATValue'] == CostVATValue) and \
                    (not VATPer or row_dict['VATPer'] == VATPer) and \
                    (not PriceWithTAX or row_dict['PriceWithTAX'] == PriceWithTAX) and \
                    (not PriceTaxableValue or row_dict['PriceTaxableValue'] == PriceTaxableValue) and \
                    (not PriceVATValue or row_dict['PriceVATValue'] == PriceVATValue) and \
                    (not PriceVATPer or row_dict['PriceVATPer'] == PriceVATPer) and \
                    (not PriceWithTAX1 or row_dict['PriceWithTAX1'] == PriceWithTAX1) and \
                    (not BookingMode or row_dict['BookingMode'] == BookingMode) and \
                    (not Pickupcodetype or row_dict['Pickupcodetype'] == Pickupcodetype) and \
                    (not Dropoffcodetype or row_dict['Dropoffcodetype'] == Dropoffcodetype):
                filtered_results.append(row_dict)

        return filtered_results

class bookingdetails(APIView):
    logger = logging.getLogger(__name__)

    def get(self, request, format=None):
        conn = database()
        cursor = conn.cursor()
        transferdate = request.data.get('transferdate', None)
        transfertype = request.data.get('transfertype', None)
        requestid = request.data.get('requestid', None)
        tlineno = request.data.get('tlineno', None)
        try:
            cursor.execute("exec [dbo].[sp_assigntransfers_get_service_details_single] ?, ?, ?, ?", (transferdate, transfertype, requestid, tlineno))
            results = cursor.fetchall()
            data = [dict(zip([column[0] for column in cursor.description], row)) for row in results]

            response_data = {'Response': 1, 'Data': data, 'Error': ""}
            self.logger.info(f"Successful Data Fetched {request.data['transferdate']} - {response_data}")
            cursor.close()
            conn.close()
            return JsonResponse(response_data, safe=False)

        except Exception as e:
            error_message = str(e)
            response_data = {'RESPONSE': 0, 'DATA': [], 'ERROR': error_message}
            self.logger.warning(f"Error During Data Fetched {request.data['transferdate']} - {response_data}")
            return JsonResponse(response_data, safe=False)
        
class costprice(APIView):
    logger = logging.getLogger(__name__)
    def put(self, request, format=None):
        conn = database()
        cursor = conn.cursor()
        data = request.data 
        requestids = data.get('requestids', None)
        transfertype = data.get('transfertype', None)
        assigntype = data.get('assigntype', None)
        partycode = data.get('partycode', None)
        remarks = data.get('remarks', None)
        confno = data.get('confno', None)
        assign_status = data.get('assign_status', None)
        costprice = data.get('costprice', None)
        overridecost = data.get('overridecost', None)
        totalsalevalue = data.get('totalsalevalue', None)
        vehicleno = data.get('vehicleno', None)
        drivercode = data.get('drivercode', None)
        drivername = data.get('drivername', None)
        drivertel1 = data.get('drivertel1', None)
        drivertel2 = data.get('drivertel2', None)
        starttime = data.get('starttime', None)
        endtime = data.get('endtime', None)
        complimentaryfromsupplier = data.get('complimentaryfromsupplier', None)
        vehiclemaxpax = data.get('vehiclemaxpax', None)
        overridemaxpax = data.get('overridemaxpax', None)
        adddate = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        adduser = data.get('adduser', None)
        moddate = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        moduser = data.get('moduser', None)
        cartype = data.get('cartype', None)
        sectorgroupcode = data.get('sectorgroupcode', None)
        costcurrcode = data.get('costcurrcode', None)
        mode = data.get('mode', None)
        assignmentid = data.get('assignmentid', None)
        service_type = data.get('ServiceType', None)
        salevalue = data.get('salevalue', None)
        parkingfee = data.get('parkingfee', None)
        totalcostprice = data.get('totalcostprice', None)

        try: 
            sql = """
                EXEC [dbo].[sp_assigntransfers_saveassignment]
                    @requestids=?, @transfertype=?, @assigntype=?, @partycode=?, @remarks=?,
                    @confno=?, @assign_status=?, @costprice=?, @overridecost=?, @totalsalevalue=?,
                    @vehicleno=?, @drivercode=?, @drivername=?, @drivertel1=?, @drivertel2=?,
                    @starttime=?, @endtime=?, @complimentaryfromsupplier=?, @vehiclemaxpax=?,
                    @overridemaxpax=?, @adddate=?, @adduser=?, @moddate=?, @moduser=?,
                    @cartype=?, @sectorgroupcode=?, @costcurrcode=?, @mode=?, @assignmentid=?,
                    @ServiceType=?, @salevalue=?, @parkingfee=?, @totalcostprice=?
            """

            cursor.execute(sql, (
                requestids, transfertype, assigntype, partycode, remarks, confno,
                assign_status, costprice, overridecost, totalsalevalue, vehicleno,
                drivercode, drivername, drivertel1, drivertel2, starttime, endtime,
                complimentaryfromsupplier, vehiclemaxpax, overridemaxpax, adddate,
                adduser, moddate, moduser, cartype, sectorgroupcode, costcurrcode,
                mode, assignmentid, service_type, salevalue, parkingfee, totalcostprice
            ))
            conn.commit()

            response_data = {'Response': 1, 'Data': 'Updated successfully', 'Error': ''}
            self.logger.info(f"Successful Data Fetched {adddate} - {response_data}")
            return JsonResponse(response_data, safe=False)

        except Exception as e:
            error_message = str(e)
            response_data = {'RESPONSE': 0, 'DATA': [], 'ERROR': error_message}
            self.logger.warning(f"Error During Data Fetched {adddate} - {response_data}")
            return JsonResponse(response_data, safe=False)
        

class transfer_assign_detail(APIView):
    logger = logging.getLogger(__name__)

    def put(self, request, format=None):
        conn = database()
        cursor = conn.cursor()
        data = request.data

        try:
            assignment_data = [
                data['assignmentid'], data['div_code'], data['requestid'], data['tlineno'],
                data['transfertype'], data['transferdate'], data['flightcode'],
                data['flight_tranid'], data['flighttime'], data['cartypecode'], data['agentcode'],
                data['pickup'], data['dropoff'], data['pickuptime'], data['roomno'],
                data['mode'], data['moduser'], data['modtime']
            ]
            sql = """
                DECLARE @table dbo.transfer_assign_detail_parameter;
                INSERT INTO @table VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                EXEC sp_assigntransfers_saveassignmentdetail
                @table = @table;
            """
            cursor.execute(sql, assignment_data)
            conn.commit()
            response_data = {'Response': 1, 'Data': 'Updated successfully', 'Error': ''}
            self.logger.info(f"Successful Data Fetched {data['requestid']} - {response_data}")
            return JsonResponse(response_data, safe=False)
        
        except Exception as e:
            error_message = str(e)
            print(error_message)
            response_data = {'RESPONSE': 0, 'DATA': [], 'ERROR': error_message}
            self.logger.warning(f"Error During Data Updating {data['requestid']} - {response_data}")
            return JsonResponse(response_data, safe=False)
        
        
class driver_duty(APIView):
    logger = logging.getLogger(__name__)
    def get(self, request,format=None):
        conn=database()
        cursor =conn.cursor()
        data = request.data
        transferdate = data.get('transferdate', None)
        drivercode = data.get('drivercode',None)

        try:
            sql='''exec [dbo].[sp_rep_driver_duty_sheet] @transferdate=?, @drivercode=?'''
            cursor.execute(sql, ( transferdate, drivercode))
            results = cursor.fetchall()
            data = [dict(zip([column[0] for column in cursor.description], row)) for row in results]
            response_data = {'Response': 1, 'Data': data, 'Error': ""}
            self.logger.info(f"Successful Data Fetched {request.data['transferdate']} - {response_data}")
            cursor.close()
            conn.close()
            return JsonResponse(response_data, safe=False)

        except Exception as e:
            error_message = str(e)
            response_data = {'RESPONSE': 0, 'DATA': [], 'ERROR': error_message}
            self.logger.warning(f"Error During Data Fetched {request.data['transferdate']} - {response_data}")
            return JsonResponse(response_data, safe=False)
        


class service_details_for_email(APIView):
    logger = logging.getLogger(__name__)
    def get(self, request,format=None):
        conn=database()
        cursor =conn.cursor()
        data = request.data
        assignmentids = data.get('assignmentids', None)

        try:
            sql='''exec [dbo].[sp_get_service_details_for_email] @assignmentids=?'''
            cursor.execute(sql, ( assignmentids))
            results = cursor.fetchall()
            data = [dict(zip([column[0] for column in cursor.description], row)) for row in results]
            response_data = {'Response': 1, 'Data': data, 'Error': ""}
            self.logger.info(f"Successful Data Fetched {request.data['assignmentids']} - {response_data}")
            cursor.close()
            conn.close()
            return JsonResponse(response_data, safe=False)
        
        except Exception as e:
            error_message = str(e)
            response_data = {'RESPONSE': 0, 'DATA': [], 'ERROR': error_message}
            self.logger.warning(f"Error During Data Fetched {request.data['assignmentids']} - {response_data}")
            return JsonResponse(response_data, safe=False)



class priortime(APIView):
    logger = logging.getLogger(__name__)
    def put(self, request, format=None):
        conn = database()
        cursor = conn.cursor()
        data = request.data
        prior_time = data.get('prior_time', None)

        try:
            if prior_time is not None:
                sql = "exec sp_executesql N'update reservation_parameters set option_selected=@PriorTime where param_id=5312', N'@PriorTime nvarchar(1)', @PriorTime=?"
                cursor.execute(sql, (prior_time,))
                conn.commit()
                response_data = {'Response': 1, 'Data': 'Updated successfully', 'Error': ''}
                self.logger.info(f"Successful Data Fetched {data.get('requestid', '')} - {response_data}")
                return JsonResponse(response_data, safe=False)
            else:
                response_data = {'RESPONSE': 0, 'DATA': [], 'ERROR': 'Prior time is required'}
                self.logger.warning(f"Prior time is required - {response_data}")
                return JsonResponse(response_data, safe=False)
            
        except Exception as e:
            error_message = str(e)
            print(error_message)
            response_data = {'RESPONSE': 0, 'DATA': [], 'ERROR': error_message}
            self.logger.warning(f"Error During Data Updating {data.get('requestid', '')} - {response_data}")
            return JsonResponse(response_data, safe=False)




class assigntransfers_getcostprice(APIView):
    logger = logging.getLogger(__name__)

    def get(self, request, format=None):
        conn = database()
        cursor = conn.cursor()
        data = request.data
        transferdate = data.get('transferdate', None)
        cartype = data.get('cartype', None)
        sectorgroupcode = data.get('sectorgroupcode', None)
        pickupcode = data.get('pickupcode', None)
        partycode = data.get('partycode', None)

        try:
            sql = '''
                exec [dbo].[sp_assigntransfers_getcostprice]@transferdate=?,@cartype=?,@sectorgroupcode=?,@pickupcode=?,@partycode=?'''

            cursor.execute(sql, (transferdate, cartype, sectorgroupcode, pickupcode, partycode))
            results = cursor.fetchall()
            data = [dict(zip([column[0] for column in cursor.description], row)) for row in results]
            response_data = {'Response': 1, 'Data': data, 'Error': ""}
            self.logger.info(f"Successful Data Fetched {request.data['transferdate']} - {response_data}")
            cursor.close()
            conn.close()
            return JsonResponse(response_data, safe=False)

        except Exception as e:
            error_message = str(e)
            response_data = {'RESPONSE': 0, 'DATA': [], 'ERROR': error_message}
            self.logger.warning(f"Error During Data Fetched {request.data['transferdate']} - {response_data}")
            return JsonResponse(response_data, safe=False)
        


class sp_executesql(APIView):
    logger = logging.getLogger(__name__)

    def get(self, request, format=None):
        conn = database()
        cursor = conn.cursor()
        data = request.data

        try:
            sptypecode_param = data.get('sptypecode_param', None)
            sptypecode_param2 = data.get('sptypecode_param2', None)
            partyname_param = data.get('partyname_param', None)

            sql = '''
                EXEC sp_executesql N'SELECT 
                    [Extent1].[partycode] AS [partycode], 
                    [Extent1].[partyname] AS [partyname], 
                    [Extent1].[add1] AS [add1], 
                    [Extent1].[add2] AS [add2], 
                    [Extent1].[add3] AS [add3], 
                    [Extent1].[tel1] AS [tel1], 
                    [Extent1].[tel2] AS [tel2], 
                    [Extent1].[fax] AS [fax], 
                    [Extent1].[contact1] AS [contact1], 
                    [Extent1].[contact2] AS [contact2], 
                    [Extent1].[email] AS [email], 
                    [Extent1].[stel1] AS [stel1], 
                    [Extent1].[stel2] AS [stel2], 
                    [Extent1].[sfax] AS [sfax], 
                    [Extent1].[scontact1] AS [scontact1], 
                    [Extent1].[scontact2] AS [scontact2], 
                    [Extent1].[semail] AS [semail], 
                    [Extent1].[atel1] AS [atel1], 
                    [Extent1].[atel2] AS [atel2], 
                    [Extent1].[afax] AS [afax], 
                    [Extent1].[acontact1] AS [acontact1], 
                    [Extent1].[acontact2] AS [acontact2], 
                    [Extent1].[aemail] AS [aemail], 
                    [Extent1].[crdays] AS [crdays], 
                    [Extent1].[crlimit] AS [crlimit], 
                    [Extent1].[catcode] AS [catcode], 
                    [Extent1].[scatcode] AS [scatcode], 
                    [Extent1].[ctrycode] AS [ctrycode], 
                    [Extent1].[citycode] AS [citycode], 
                    [Extent1].[currcode] AS [currcode], 
                    [Extent1].[automail] AS [automail], 
                    [Extent1].[agtcode] AS [agtcode], 
                    [Extent1].[general] AS [general], 
                    [Extent1].[accrualacctcode] AS [accrualacctcode], 
                    [Extent1].[controlacctcode] AS [controlacctcode], 
                    [Extent1].[cashsupp] AS [cashsupp], 
                    [Extent1].[checkprint] AS [checkprint], 
                    [Extent1].[active] AS [active], 
                    [Extent1].[website] AS [website], 
                    [Extent1].[Preferred] AS [Preferred], 
                    [Extent1].[sectorcode] AS [sectorcode], 
                    [Extent1].[postaccount] AS [postaccount], 
                    [Extent1].[rnkorder] AS [rnkorder], 
                    [Extent1].[adddate] AS [adddate], 
                    [Extent1].[adduser] AS [adduser], 
                    [Extent1].[moddate] AS [moddate], 
                    [Extent1].[moduser] AS [moduser], 
                    [Extent1].[cencelpolicy] AS [cencelpolicy], 
                    [Extent1].[generalpolicy] AS [generalpolicy], 
                    [Extent1].[mobileno] AS [mobileno], 
                    [Extent1].[smobileno] AS [smobileno], 
                    [Extent1].[amobileno] AS [amobileno], 
                    [Extent1].[postingtype] AS [postingtype], 
                    [Extent1].[mapcode] AS [mapcode], 
                    [Extent1].[showinweb] AS [showinweb], 
                    [Extent1].[areacode] AS [areacode], 
                    [Extent1].[invpost] AS [invpost], 
                    [Extent1].[invaccount] AS [invaccount], 
                    [Extent1].[hotelchaincode] AS [hotelchaincode], 
                    [Extent1].[hotelstatuscode] AS [hotelstatuscode], 
                    [Extent1].[propertytype] AS [propertytype], 
                    [Extent1].[VATexclude] AS [VATexclude], 
                    [Extent1].[ServiceChargePerc] AS [ServiceChargePerc], 
                    [Extent1].[MunicipalityFeePerc] AS [MunicipalityFeePerc], 
                    [Extent1].[TourismFeePerc] AS [TourismFeePerc], 
                    [Extent1].[VATPerc] AS [VATPerc], 
                    [Extent1].[Hotel_Account_Name] AS [Hotel_Account_Name], 
                    [Extent1].[Hotel_Account_Number] AS [Hotel_Account_Number], 
                    [Extent1].[Hotel_Account_Banck_Name] AS [Hotel_Account_Banck_Name], 
                    [Extent1].[Hotel_Account_Branch_Name] AS [Hotel_Account_Branch_Name], 
                    [Extent1].[Hotel_Account_SWIFT] AS [Hotel_Account_SWIFT], 
                    [Extent1].[Hotel_Account_IBAN] AS [Hotel_Account_IBAN], 
                    [Extent1].[Hotel_Account_Currency] AS [Hotel_Account_Currency], 
                    [Extent1].[TRNNo] AS [TRNNo]
                FROM [dbo].[partymast] AS [Extent1]
                WHERE (([Extent1].[sptypecode] = @p__linq__0) OR 
                       ([Extent1].[sptypecode] IS NULL AND @p__linq__0 IS NULL) OR 
                       ([Extent1].[sptypecode] = @p__linq__1) OR 
                       ([Extent1].[sptypecode] IS NULL AND @p__linq__1 IS NULL)) AND 
                      ([Extent1].[partyname] LIKE @p__linq__2 ESCAPE ''~'')',
                N'@p__linq__0 varchar(8000), @p__linq__1 varchar(8000), @p__linq__2 varchar(8000)',
                @p__linq__0=?, @p__linq__1=?, @p__linq__2=?
            '''

            cursor.execute(sql, (sptypecode_param, sptypecode_param2, partyname_param))
            results = cursor.fetchall()

            data = [dict(zip([column[0] for column in cursor.description], row)) for row in results]

            response_data = {'Response': 1, 'Data': data, 'Error': ""}
            self.logger.info(f"Successful Data Fetched - {response_data}")

            cursor.close()
            conn.close()
            return JsonResponse(response_data, safe=False)

        except Exception as e:
            error_message = str(e)
            response_data = {'RESPONSE': 0, 'DATA': [], 'ERROR': error_message}
            self.logger.warning(f"Error During Data Fetch - {response_data}")
            return JsonResponse(response_data, safe=False)
