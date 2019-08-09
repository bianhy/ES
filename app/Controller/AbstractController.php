<?php

namespace App\Controller;

use Amber\System\Controller;

class AbstractController extends Controller
{


    public function __construct()
    {
        parent::__construct();
    }

    protected function getToken($key, $default = null)
    {
        if (!isset($_REQUEST[$key])) {
            $val = $default;
            if (isset($_COOKIE[$key])) {
                $val = $_COOKIE[$key];
            }
        } else {
            $val = urldecode($_REQUEST[$key]);
            $this->setToken($key, $val);
        }
        return $val;
    }

    protected function setToken($key, $value, $expire = null)
    {
        header('P3P: CP="NOI DEV PSA PSD IVA PVD OTP OUR OTR IND OTC"');
        setcookie($key, $value, $expire, '/');
    }
}
