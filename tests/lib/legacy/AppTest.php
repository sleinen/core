<?php

/**
 *
 * @author Jörn Friedrich Dreyer <jfd@butonic.de>
 * @copyright Copyright (c) 2017, ownCloud GmbH.
 * @license AGPL-3.0
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU AFFERO GENERAL PUBLIC LICENSE
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU AFFERO GENERAL PUBLIC LICENSE for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this library.  If not, see <http://www.gnu.org/licenses/>.
 *
 */


namespace Test\legacy;

function rrmdir($directory) {
	$files = array_diff(scandir($directory), ['.','..']);
	foreach ($files as $file) {
		if (is_dir($directory . '/' . $file)) {
			rrmdir($directory . '/' . $file);
		} else {
			unlink($directory . '/' . $file);
		}
	}
	return rmdir($directory);
}


class AppTest extends \Test\TestCase {

	private $appPath;

	protected function setUp() {
		parent::setUp();

		$this->appPath = __DIR__ . '/../../../apps/appinfotestapp';
		$infoXmlPath = $this->appPath . '/appinfo/info.xml';
		mkdir($this->appPath . '/appinfo', 0777, true);

		\OC_App::clearAppCache('appinfotestapp');
		\OC_App::clearAppCache($this->appPath.'/appinfo/info.xml');

		$xml = '<?xml version="1.0" encoding="UTF-8"?>' .
		'<info>' .
		    '<id>appinfotestapp</id>' .
			'<namespace>AppInfoTestApp</namespace>' .
		'</info>';
		file_put_contents($infoXmlPath, $xml);
	}

	public function testGetAppInfo() {
		$info = \OC_App::getAppInfo('appinfotestapp');
		self::assertArraySubset(
			[
				'id'=>'appinfotestapp',
				'namespace' => 'AppInfoTestApp',
				'_cached' => false,
			],
			$info
		);

		// now it should be cached
		$info2 = \OC_App::getAppInfo('appinfotestapp');
		self::assertArraySubset(
			[
				'id'=>'appinfotestapp',
				'namespace' => 'AppInfoTestApp',
				'_cached' => true,
			],
			$info2
		);
	}

	public function testGetAppInfoByIdFillsCacheForPath() {
		$info = \OC_App::getAppInfo('appinfotestapp');
		self::assertArraySubset(
			[
				'id'=>'appinfotestapp',
				'namespace' => 'AppInfoTestApp',
				'_cached' => false,
			],
			$info
		);

		// should be cached, even if fetching by path
		$info2 = \OC_App::getAppInfo($this->appPath.'/appinfo/info.xml', true);
		self::assertArraySubset(
			[
				'id'=>'appinfotestapp',
				'namespace' => 'AppInfoTestApp',
				'_cached' => true,
			],
			$info2
		);
	}

	public function testGetAppInfoByPathFillsCacheForAppId() {
		$info = \OC_App::getAppInfo($this->appPath.'/appinfo/info.xml', true);
		self::assertArraySubset(
			[
				'id'=>'appinfotestapp',
				'namespace' => 'AppInfoTestApp',
				'_cached' => false,
			],
			$info
		);

		// should be cached, even if fetching by appid
		$info2 = \OC_App::getAppInfo('appinfotestapp');
		self::assertArraySubset(
			[
				'id'=>'appinfotestapp',
				'namespace' => 'AppInfoTestApp',
				'_cached' => true,
			],
			$info2
		);
	}

	public function testGetAppInfoXMLChange() {
		$info = \OC_App::getAppInfo('appinfotestapp');
		self::assertArraySubset(
			[
				'id'=>'appinfotestapp',
				'namespace' => 'AppInfoTestApp',
				'_cached' => false,
			],
			$info
		);

		// change app namespace
		$infoXmlPath = $this->appPath . '/appinfo/info.xml';
		$xml = '<?xml version="1.0" encoding="UTF-8"?>' .
			'<info>' .
			'<id>appinfotestapp</id>' .
			'<namespace>AppInfoTestApp2</namespace>' .
			'</info>';
		file_put_contents($infoXmlPath, $xml);

		// should return new namespace
		$info2 = \OC_App::getAppInfo('appinfotestapp');
		self::assertArraySubset(
			[
				'id'=>'appinfotestapp',
				'namespace' => 'AppInfoTestApp2',
				'_cached' => false,
			],
			$info2
		);

		// now it should be cached
		$info3 = \OC_App::getAppInfo('appinfotestapp');
		self::assertArraySubset(
			[
				'id'=>'appinfotestapp',
				'namespace' => 'AppInfoTestApp2',
				'_cached' => true,
			],
			$info3
		);
	}

	public function testGetAppInfoPathChange() {
		// store info in a different file
		$infoXmlPath = $this->appPath . '/appinfo/info-old.xml';
		$xml = '<?xml version="1.0" encoding="UTF-8"?>' .
			'<info>' .
			'<id>appinfotestapp</id>' .
			'<namespace>AppInfoTestApp</namespace>' .
			'</info>';
		file_put_contents($infoXmlPath, $xml);

		// fill cache with 'old' path
		$info = \OC_App::getAppInfo($infoXmlPath, true);
		self::assertArraySubset(
			[
				'id'=>'appinfotestapp',
				'namespace' => 'AppInfoTestApp',
				'_cached' => false,
			],
			$info
		);

		unlink($infoXmlPath);

		// check info can be found under new path by using the appid
		$info2 = \OC_App::getAppInfo('appinfotestapp');
		self::assertArraySubset(
			[
				'id'=>'appinfotestapp',
				'namespace' => 'AppInfoTestApp',
				'_cached' => false,
			],
			$info2
		);

		// now it should be cached
		$info3 = \OC_App::getAppInfo('appinfotestapp');
		self::assertArraySubset(
			[
				'id'=>'appinfotestapp',
				'namespace' => 'AppInfoTestApp',
				'_cached' => true,
			],
			$info3
		);
	}

	public function testGetAppInfoEmpty() {
		self::assertNull(\OC_App::getAppInfo(''));
		self::assertNull(\OC_App::getAppInfo('_notexistingfortest'));
		self::assertNull(\OC_App::getAppInfo($this->appPath.'/appinfo/info-not-existing.xml'), true);

		$infoXmlPath = $this->appPath . '/appinfo/info.xml';
		file_put_contents($infoXmlPath, '');
		self::assertNull(\OC_App::getAppInfo('appinfotestapp'));
	}

	public function testGetAppInfoOCSIDReplacement() {

		// the app was installed when it had a different ocsid
		\OC::$server->getConfig()->setAppValue('appinfotestapp', 'ocsid', 'oldocsidfromdb');

		// store info with ocsid
		$infoXmlPath = $this->appPath . '/appinfo/info.xml';
		$xml = '<?xml version="1.0" encoding="UTF-8"?>' .
			'<info>' .
			'<id>appinfotestapp</id>' .
			'<namespace>AppInfoTestApp</namespace>' .
			'<ocsid>newid</ocsid>' .
			'</info>';
		file_put_contents($infoXmlPath, $xml);

		$info = \OC_App::getAppInfo('appinfotestapp');
		self::assertArraySubset(
			[
				'id'=>'appinfotestapp',
				'namespace' => 'AppInfoTestApp',
				'ocsid' => 'oldocsidfromdb',
				'_cached' => false,
			],
			$info
		);

		// now it should be cached
		$info2 = \OC_App::getAppInfo('appinfotestapp');
		self::assertArraySubset(
			[
				'id'=>'appinfotestapp',
				'namespace' => 'AppInfoTestApp',
				'ocsid' => 'oldocsidfromdb',
				'_cached' => true,
			],
			$info2
		);

		\OC::$server->getConfig()->deleteAppValue('appinfotestapp', 'ocsid');
	}

	protected function tearDown() {
		\OC_App::clearAppCache('appinfotestapp');
		\OC_App::clearAppCache($this->appPath.'/appinfo/info.xml');
		rrmdir($this->appPath);
		parent::tearDown();
	}

}
