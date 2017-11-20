import requests
import psycopg2

from Queue import Queue
from threading import Thread
from bs4 import BeautifulSoup

URL = ""
USER = ""
PASSWORD = ""


def auth(session):
    """Login Clarin"""
    form = {"password": PASSWORD,
            "username": USER,
            "remember-me": 'on'}
    return session.post(URL, data=form)

if __name__ == "__main__":
    session = requests.session()
    response = auth(session)
    if response.status_code != 200:
        raise

    con = psycopg2.connect("dbname= user= password=")
    cur = con.cursor()
    # Queue 10 at a time
    url_queue = Queue(10)

    def worker():
        """Gets the next url from the queue and processes it"""
        while True:

            url = url_queue.get()
            print url
            cv = session.get(url)
            # Remove private candidates
            if 'Candidato Secreto' in cv.content:
                continue
            soup = BeautifulSoup(cv.content, "html.parser")

            age = dni = 0
            contact_info = image_url = country = state = address = the_rest = ''

            has_img = soup.find_all('li', class_='picture-row')
            if len(has_img):
                image = has_img[0].img.get('src')
                image_url = "https://www.empleos.clarin.com" + image

            has_contact_info = soup.find_all('li', class_='contact-info')
            if len(has_contact_info):
                contact_info = has_contact_info[0]

            name = contact_info.h3.text
            mail_and_phone = contact_info.find_all('p')
            mail = mail_and_phone[0].text
            phone = mail_and_phone[1].text

            postulant_info = soup.find_all('li', class_='postulant-info')[0].find_all('li')
            if len(postulant_info):
                age = int(postulant_info[0].text.split(' ')[0])
            elif len(postulant_info) > 1:
                dni = int(postulant_info[1].text.split('DNI')[1])
                country = postulant_info[-1].text
            elif len(postulant_info) > 2:
                address = postulant_info[3].text
                state = postulant_info[-2].text
            elif len(postulant_info) > 3:
                for info in postulant_info[4:-2]:
                    the_rest += info.text

            url_split = url.replace('https://www.empleos.clarin.com/empresas/cv/', '')
            urla = url_split.replace('?q=&t=all', '')
            cv_id = int(urla)
            cur.execute("INSERT INTO cvs (cv_id, image, "
                        "name, mail, phone, age, dni, address, "
                        "country, state, additional) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (cv_id, image_url, name, mail, phone, age, dni,
                         address, country, state, the_rest))
            url_queue.task_done()

    # Start a pool of 40 workers
    # This is to speed up HTTP requests - Depends on Server Stats
    for i in xrange(40):
        t = Thread(target=worker)
        t.daemon = True
        t.start()

    # Start and end UID MAX = 2.5mm
    start = 0
    end = 10000
    for i in xrange(start, end):
        generate_url = "https://www.empleos.clarin.com/empresas/cv/" + str(i) + "?q=&t=all"
        url_queue.put(generate_url)
    # Block until everything is finished.
    url_queue.join()
    # Commit onto database
    con.commit()
