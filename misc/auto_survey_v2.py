from selenium import webdriver
import random
import pandas as pd
import time
import sys

PAGE = "http://ncssle.sanametrix.com/"
FILENAME = sys.argv[1]


def get_rand_num(num):
    return str(random.randint(0, num-1))


def fill_questions():
    # Check which type of question

    if len(demog) > 0:
        # Demographic question
        options = driver.find_element_by_class_name("radio_block").find_elements_by_tag_name("li")
        if len(options) < 6:
            num = get_rand_num(len(options))
        else:
            num = str(random.randint(5, 13))
        driver.find_element_by_id('ResponseResponse' + num).click()
        driver.find_element_by_id('btn_next').click()
        return True

    elif len(ethn) > 0:
        # Ethnicity question (1 - 5 choices, multiple possible)
        total_num_race = random.choices([1, 2, 3, 4, 5], weights=[5, 4, 1, .5, .25], k=1)[0]
        choices = random.sample([1, 2, 3, 4, 5], total_num_race)
        for choice in choices:
            driver.find_elements_by_css_selector('.checkbox input')[int(choice)].click()
        driver.find_element_by_id('btn_next').click()
        return True

    elif len(survey) > 0:
        # Survey question
        options = driver.find_element_by_class_name("radio_inline").find_elements_by_tag_name("li")

        if len(options) == 5:
            num = str(random.randint(0, 4))
        else:
            num = get_rand_num(len(options))
        #driver.find_element_by_id('ResponseResponse' + num).click()
        driver.find_elements_by_class_name('block')[int(num)].click()
        driver.find_element_by_id('btn_next').click()
        return True

    else:
        # Not a survey question
        return False

# Open csv of usernames
df = pd.read_csv(FILENAME)
usernames = df['USERNAME']
print(usernames)

for user in usernames:
    driver = webdriver.Chrome()
    driver.get(PAGE)
    print("Opened main page. Waiting 2 seconds for the page to load completely.")
    time.sleep(2)
    # Login
    driver.find_element_by_id("username").clear()
    driver.find_element_by_id("username").send_keys(user)
    driver.find_element_by_id("btn_log_in").click()
    # Consent
    print("Opened consent page. Waiting 2 seconds for the page to load completely.")
    time.sleep(2)
    driver.find_element_by_id("btn_consent_yes").click()
    # Continue
    print("Opened continue page. Waiting 2 seconds for the page to load completely.")
    time.sleep(2)
    driver.find_element_by_css_selector(
        ".button.ui-button.ui-widget.ui-state-default.ui-corner-all.ui-button-text-only").click()
    print("Opened survey. Waiting 2 seconds for the page to load completely.")
    time.sleep(2)
    # Questions

    while True:
        try:
            demog = driver.find_elements_by_class_name("radio_block")
            ethn = driver.find_elements_by_class_name("checkbox")
            survey = driver.find_elements_by_class_name("radio_inline")

            if(demog or ethn or survey):
                fill_questions()
            else:
                break
        except:
            continue

    # User is printed after completion
    print("Finished entering survey for user: {}".format(user))
    driver.quit()
    # Wait 6 seconds to reset
    print("Opened main page. Taking a break for 5 seconds")
    time.sleep(5)
