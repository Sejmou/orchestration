import requests


def get_public_ip_from_ipinfo() -> str:
    """
    Gets the public IP address of the host machine from ipinfo.io.
    """
    response = requests.get("https://ipinfo.io/json", timeout=3)
    data = response.json()
    return data["ip"]


def get_public_ip_from_ifconfig() -> str:
    """
    Gets the public IP address of the host machine from the API endpoint at ifconfig.io.
    """
    response = requests.get("https://ifconfig.io/ip", timeout=3)
    return response.text.strip()


def get_public_ip_from_ipify() -> str:
    """
    Gets the public IP address of the host machine from the API endpoint at ipify.org.
    """
    response = requests.get("https://api.ipify.org?format=json", timeout=3)
    data = response.json()
    return data["ip"]


def get_public_ip_from_icanhazip() -> str:
    """
    Gets the public IP address of the host machine from the API endpoint at icanhazip.com.
    """
    response = requests.get("https://icanhazip.com", timeout=3)
    return response.text.strip()


def get_public_ip() -> str:
    """
    Gets the public IP address of the host machine. Tries multiple services until one works.
    """
    try:
        return get_public_ip_from_ifconfig()
    except Exception:
        pass

    try:
        return get_public_ip_from_icanhazip()
    except Exception:
        pass

    try:
        return get_public_ip_from_ipinfo()
    except Exception:
        pass

    try:
        return get_public_ip_from_ipify()
    except Exception:
        pass

    raise Exception("Could not get public IP address from any service.")


if __name__ == "__main__":
    ips = [
        ("ifconfig", get_public_ip_from_ifconfig()),
        ("ipinfo", get_public_ip_from_ipinfo()),
        ("ipify", get_public_ip_from_ipify()),
        ("icanhazip", get_public_ip_from_icanhazip()),
        ("get_public_ip", get_public_ip()),
    ]

    for name, ip in ips:
        print(f"{name} IP address: {ip}")
