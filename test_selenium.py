from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

def main():
    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-software-rasterizer")

        # Chrome 경로는 /usr/bin/google-chrome (심볼릭 링크)
        chrome_options.binary_location = "/usr/bin/google-chrome"

        # ChromeDriver는 /usr/local/bin/chromedriver
        service = Service("/usr/local/bin/chromedriver")

        driver = webdriver.Chrome(service=service, options=chrome_options)

        driver.get("https://www.google.com")
        print("Page title......:", driver.title)

        driver.quit()

    except Exception as e:
        import traceback
        print("🚨 Selenium 실행 중 예외 발생:")
        traceback.print_exc()

if __name__ == "__main__":
    main()
