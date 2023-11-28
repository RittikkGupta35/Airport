from django.urls import path
from login.views import *

urlpatterns = [
    path('verify/', verify_user.as_view(), name='verify_user'),
    path('bookings/', bookings.as_view(), name='bookings'),
    path('bookingdetails/', bookingdetails.as_view(), name='bookingdetails'),
    path('costprice/', costprice.as_view(), name='costprice'),
    path('transfer_assign_detail/', transfer_assign_detail.as_view(), name='transfer_assign_detail'),
    path('driver_duty/', driver_duty.as_view(), name='driver_duty'),
    path('service_details_for_email/', service_details_for_email.as_view(), name='service_details_for_email'),
    path('priortime/', priortime.as_view(), name='priortime'),

    path('sp_executesql/', sp_executesql.as_view(), name='sp_executesql'),
    path('assigntransfers_getcostprice/', assigntransfers_getcostprice.as_view(), name='assigntransfers_getcostprice'),
]