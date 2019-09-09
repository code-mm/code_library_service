import signal, time, sys, os, logging, psycopg2
from datetime import date
from smtplib import SMTP, SMTPConnectError, SMTPHeloError, SMTPServerDisconnected, SMTPResponseException, SMTPSenderRefused, SMTPRecipientsRefused, SMTPDataError

# logging
logging.basicConfig(level=logging.DEBUG)

# handle sigterm
LOOP_TIMER = 10
run = True

def handler_stop_signals(signum, frame):
    global run
    run = False

signal.signal(signal.SIGINT, handler_stop_signals)
signal.signal(signal.SIGTERM, handler_stop_signals)

# database connection
db_hostname = os.environ.get('db_hostname')
db_port = os.environ.get('db_port')
db_timeout = os.environ.get('db_timeout')
db_name = os.environ.get('db_name')
db_username = os.environ.get('db_username')
db_password = os.environ.get('db_password')

conn = None

logging.info("Service is starting")
try:
    conn = psycopg2.connect(host=db_hostname, port=db_port, dbname=db_name, user=db_username, password=db_password)
except (psycopg2.DatabaseError, psycopg2.OperationalError):
    logging.error("Database connection error")
    logging.error("Exiting...")
    sys.exit(1)
cursor = conn.cursor()

# initialize email server
email_server = SMTP()


# runtime loop
logging.info("Entering runtime loop")
while run:
    # connect to email server
    try:
        email_server.connect()
    except:
        logging.error('Could not connect to email server')

    # move reserved loans, if free
    cursor.execute("SELECT move_reserved_loans();")
    moved_reservations = cursor.fetchone()
    conn.commit()
    logging.info('moved {} reservations'.format(moved_reservations[0]))

    # inform user about reserved loans
    cursor.execute("SELECT book_loanreserved.id, book_loanreserved.duration, auth_user.first_name, auth_user.last_name, auth_user.email, book_book.title1, book_book.title2, book_book.author FROM book_loanreserved INNER JOIN auth_user ON book_loanreserved.user_id = auth_user.id INNER JOIN book_bookcopies ON book_loanreserved.book_copy_id = book_bookcopies.id INNER JOIN book_book ON book_bookcopies.book_id = book_book.id WHERE book_loanreserved.reservation_information = false;")
    new_reserved_loans = cursor.fetchall()
    if len(new_reserved_loans):
        for loan in new_reserved_loans:
            try:
                email_server.sendmail(from_addr='library-manager@code.berlin', to_addrs=loan[4], msg='New reserved book {} - {} from author(s) {}.'.format(loan[5], loan[6], loan[7]))
                cursor.execute('UPDATE book_loanreserved SET reservation_information = True WHERE id = {};'.format(loan[0]))
                conn.commit()
                logging.info('send new reservation email to {}'.format(loan[4]))
            except (SMTPServerDisconnected, SMTPResponseException, SMTPSenderRefused, SMTPRecipientsRefused, SMTPDataError):
                logging.error('Could not send reserved book information email to {}'.format(loan[4]))
            except (psycopg2.DatabaseError, psycopg2.OperationalError):
                logging.error('Could not write book reservation information to database, for user {}'.format(loan[4]))
            except:
                logging.error('Unknown error in reserved loans')

    # inform user about new active loans
    cursor.execute("SELECT book_loan.id, book_loan.from_date, book_loan.to_date, book_loan.loan_start_information, book_loan.loan_end_information, auth_user.email, book_book.title1, book_book.title2, book_book.author FROM book_loan INNER JOIN auth_user ON book_loan.user_id = auth_user.id INNER JOIN book_bookcopies ON book_loan.book_copy_id = book_bookcopies.id INNER JOIN book_book ON book_bookcopies.book_id = book_book.id WHERE from_date <= '{}'::date AND to_date > '{}'::date AND loan_start_information = false;".format(date.today().strftime('%Y-%m-%d'), date.today().strftime('%Y-%m-%d')))
    new_loans = cursor.fetchall()
    if len(new_loans):
        for loan in new_loans:
            try:
                email_server.sendmail(from_addr='library-manager@code.berlin', to_addrs=loan[5], msg='Book {} - {} from author(s) {} available for pickup in library.'.format(loan[6], loan[7], loan[8]))
                cursor.execute('UPDATE book_loan SET loan_start_information = True WHERE id = {};'.format(loan[0]))
                conn.commit()
                logging.info('send new loan email to {}'.format(loan[5]))
            except (SMTPServerDisconnected, SMTPResponseException, SMTPSenderRefused, SMTPRecipientsRefused, SMTPDataError):
                logging.error('Could not send new book loan email to {}'.format(loan[5]))
            except (psycopg2.DatabaseError, psycopg2.OperationalError):
                logging.error('Could not write book start information to database, for user {}'.format(loan[5]))
            except:
                logging.error('Unknown error in new active loans')

    # inform user about loans to be returned today
    cursor.execute("SELECT book_loan.id, book_loan.from_date, book_loan.to_date, book_loan.loan_start_information, book_loan.loan_end_information, auth_user.email, book_book.title1, book_book.title2, book_book.author FROM book_loan INNER JOIN auth_user ON book_loan.user_id = auth_user.id INNER JOIN book_bookcopies ON book_loan.book_copy_id = book_bookcopies.id INNER JOIN book_book ON book_bookcopies.book_id = book_book.id WHERE to_date = '{}'::date AND loan_end_information = false;".format(date.today().strftime('%Y-%m-%d')))
    return_loans = cursor.fetchall()
    if len(return_loans):
        for loan in return_loans:
            try:
                email_server.sendmail(from_addr='library-manager@code.berlin', to_addrs=loan[5], msg='Book {} - {} from author(s) {} must be returned to the library today.'.format(loan[6], loan[7], loan[8]))
                cursor.execute('UPDATE book_loan SET loan_end_information = True WHERE id = {};'.format(loan[0]))
                conn.commit()
                logging.info('send email to {}'.format(loan[5]))
            except (SMTPServerDisconnected, SMTPResponseException, SMTPSenderRefused, SMTPRecipientsRefused, SMTPDataError):
                logging.error('Could not send return book loan email to {}'.format(loan[5]))
            except (psycopg2.DatabaseError, psycopg2.OperationalError):
                logging.error('Could not write book end information to database, for user {}'.format(loan[5]))
            except:
                logging.error('Unknown error in running out loans')

    # disconnect from email server
    try:
        email_server.quit()
    except:
        logging.error('Could not disconnect from email server')

    time.sleep(LOOP_TIMER)
