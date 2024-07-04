CREATE OR REPLACE PROCEDURE encryption_1(customerid VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
    var arr = CustomerId.toString();
    var n = arr.length;
    var customerid = '';

    if (n == 10) {
        customerid = arr[6] + arr[4] + arr[7] + arr[5] + arr[1] + arr[3] + arr[0] + arr[2] + arr[8] + arr[9];
    } else if (n == 9) {
        customerid = arr[5] + arr[3] + arr[6] + arr[4] + arr[0] + arr[2] + '0' + arr[1] + arr[7] + arr[8];
    } else if (n == 8) {
        customerid = arr[4] + arr[2] + arr[5] + arr[3] + '0' + arr[1] + '0' + arr[0] + arr[6] + arr[7];
    } else if (n == 7) {
        customerid = arr[3] + arr[1] + arr[4] + arr[2] + '0' + arr[0] + '0' + '0' + arr[5] + arr[6];
    } else if (n == 6) {
        customerid = arr[2] + arr[0] + arr[3] + arr[1] + '0' + '0' + '0' + '0' + arr[4] + arr[5];
    } else if (n == 5) {
        customerid = arr[1] + '0' + arr[2] + arr[0] + '0' + '0' + '0' + '0' + arr[3] + arr[4];
    } else if (n == 4) {
        customerid = arr[0] + '0' + arr[1] + '0' + '0' + '0' + '0' + '0' + arr[2] + arr[3];
    } else if (n == 3) {
        customerid = '0' + '0' + arr[0] + '0' + '0' + '0' + '0' + '0' + arr[1] + arr[2];
    } else if (n == 2) {
        customerid = '0' + '0' + '0' + '0' + '0' + '0' + '0' + '0' + arr[0] + arr[1];
    } else if (n == 1) {
        customerid = '0' + '0' + '0' + '0' + '0' + '0' + '0' + '0' + '0' + arr[0];
    } else {
        customerid = null;
    }

    return customerid;
$$;
