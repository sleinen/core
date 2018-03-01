<?php

use GuzzleHttp\Client;
use GuzzleHttp\Exception\BadResponseException;
use GuzzleHttp\Psr7\Request;

require __DIR__ . '/../../../../lib/composer/autoload.php';

trait Auth {

	private $clientToken;

	/** @BeforeScenario */
	public function setUpScenario() {
		$this->client = new Client();
		$this->responseXml = '';
	}

	/**
	 * @When a user requests :url with :method and no authentication
	 * @Given a user has requested :url with :method and no authentication
	 *
	 * @param string $url
	 * @param string $method
	 */
	public function userRequestsURLWith($url, $method) {
		$this->sendRequest($url, $method);
	}

	/**
	 * @param string $url
	 * @param string $method
	 * @param string|null $authHeader
	 * @param bool $useCookies
	 */
	private function sendRequest($url, $method, $authHeader = null, $useCookies = false) {
		$fullUrl = substr($this->baseUrl, 0, -5) . $url;
		$options = [];
		try {
			$headers = [
				'OCS_APIREQUEST' => 'true',
				'requesttoken' => $this->requestToken
			];
			if ($authHeader) {
				$headers['Authorization'] = $authHeader;
			}
			if ($useCookies) {
				$options = [
				    'cookies' => $this->cookieJar,
				];
			}
			if ($authHeader) {
				$headers['Authorization'] = $authHeader;
			}
			$headers['OCS_APIREQUEST'] = 'true';
			$headers['requesttoken'] = $this->requestToken;
			$request = new Request($method, $fullUrl, $headers);
			$this->response = $this->client->send($request, $options);
		} catch (BadResponseException $ex) {
			$this->response = $ex->getResponse();
		}
	}

	/**
	 * @When user :user generates a new client token using the API
	 * @Given a new client token for :user has been generated
	 *
	 * @param string $user
	 */
	public function aNewClientTokenHasBeenGenerated($user) {
		$client = new Client();
		$resp = $client->post(substr($this->baseUrl, 0, -5) . '/token/generate', [
			'json' => [
					'user' => $user,
					'password' => $this->getPasswordForUser($user),
			]
		]);
		$this->clientToken = json_decode($resp->getBody()->getContents())->token;
	}

	/**
	 * @When user :user requests :url with :method using basic auth
	 * @Given user :user has requested :url with :method using basic auth
	 *
	 * @param string $user
	 * @param string $url
	 * @param string $method
	 */
	public function userRequestsURLWithUsingBasicAuth($user, $url, $method) {
		$authString = $user . ':' . $this->getPasswordForUser($user);
		$this->sendRequest($url, $method, 'basic ' . base64_encode($authString));
	}

	/**
	 * @When user :user requests :url with :method using basic token auth
	 * @Given user :user has requested :url with :method using basic token auth
	 *
	 * @param string $user
	 * @param string $url
	 * @param string $method
	 */
	public function userRequestsURLWithUsingBasicTokenAuth($user, $url, $method) {
		$this->sendRequest($url, $method, 'basic ' . base64_encode($user . ':' . $this->clientToken));
	}

	/**
	 * @When the user requests :url with :method using the generated client token
	 * @Given the user has requested :url with :method using the generated client token
	 *
	 * @param string $url
	 * @param string $method
	 */
	public function userRequestsURLWithUsingAClientToken($url, $method) {
		$this->sendRequest($url, $method, 'token ' . $this->clientToken);
	}

	/**
	 * @When the user requests :url with :method using the browser session
	 * @Given the user has requested :url with :method using the browser session
	 *
	 * @param string $url
	 * @param string $method
	 */
	public function userRequestsURLWithBrowserSession($url, $method) {
		$this->sendRequest($url, $method, null, true);
	}

	/**
	 * @Given a new browser session for :user has been started
	 *
	 * @param string $user
	 */
	public function aNewBrowserSessionForHasBeenStarted($user) {
		$loginUrl = substr($this->baseUrl, 0, -5) . '/login';
		// Request a new session and extract CSRF token
		$client = new Client();
		$response = $client->get(
			$loginUrl, [
		    'cookies' => $this->cookieJar,
			]
		);
		$this->extracRequestTokenFromResponse($response);

		// Login and extract new token
		$client = new Client();
		$response = $client->post(
			$loginUrl, [
				'form_params' => [
					'user' => $user,
					'password' => $this->getPasswordForUser($user),
					'requesttoken' => $this->requestToken,
				],
				'cookies' => $this->cookieJar,
			]
		);
		$this->extracRequestTokenFromResponse($response);
	}

}
