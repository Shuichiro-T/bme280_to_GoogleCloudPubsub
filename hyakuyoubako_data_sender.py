#!/usr/bin/env python
# -*- coding: utf-8 -*-

#HYAKUYOBAKO DATA SENDER
"""
"""

import smbus2
import bme280

import argparse
import base64
import datetime
import json
import threading
import time

import jwt
import requests

_BASE_URL = 'https://cloudiotdevice.googleapis.com/v1'
bus_number = 1
i2c_address = 0x76

bus = smbus2.SMBus(bus_number)

calibration_params = bme280.load_calibration_params(bus, i2c_address)


def readData():
    data = bme280.sample(bus, i2c_address, calibration_params)

    datas = {'temperature':data.temperature ,\
            'pressure':data.pressure, 'humidity':data.humidity}

    return datas


def create_message(id, logitude, latitude):
    datas = readData()

    #送信するメッセージをJSON形式にする
    message = '{{\
        "ID":{},\
        "LOCATION_LOGI":{},\
        "LOCATION_LATI":{},\
        "DEVICE_DATETIME":"{}",\
        "TEMPERATURE":{},\
        "PRESSURE":{},\
        "HUMIDITY":{}}}'                        .format(id, logitude, latitude, datetime.datetime.now().\
            strftime('%Y-%m-%dT%H:%M:%S'),datas['temperature'] ,datas['pressure'] ,datas['humidity'])
    return message


def create_jwt(project_id, private_key_file, algorithm):
    token = {
        # The time the token was issued.
        'iat': datetime.datetime.utcnow(),
        # Token expiration time.
        'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=60),
        # The audience field should always be set to the GCP project id.
        'aud': project_id
    }

    # Read the private key file.
    with open(private_key_file, 'r') as f:
        private_key = f.read()

    print('Creating JWT using {} from private key file {}'.format(
        algorithm, private_key_file))

    return jwt.encode(token, private_key, algorithm=algorithm).decode('ascii')


def publish_message(message, message_type, base_url, project_id, cloud_region,
                    registry_id, device_id, jwt_token):
    headers = {
        'authorization': 'Bearer {}'.format(jwt_token),
        'content-type': 'application/json',
        'cache-control': 'no-cache'
    }

    # Publish to the events or state topic based on the flag.
    url_suffix = 'publishEvent' if message_type == 'event' else 'setState'

    publish_url = (
        '{}/projects/{}/locations/{}/registries/{}/devices/{}:{}').format(
            base_url, project_id, cloud_region, registry_id, device_id,
            url_suffix)

    #print('Publishing URL : \'{}\''.format(publish_url))

    body = None
    msg_bytes = base64.urlsafe_b64encode(message.encode('utf-8'))
    if message_type == 'event':
        body = {'binary_data': msg_bytes.decode('ascii')}
    else:
        body = {'state': {'binary_data': base64.urlsafe_b64encode(message)}}

    resp = requests.post(publish_url, data=json.dumps(body), headers=headers)

    return resp


def parse_command_line_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description=('HYAKUYOBAKO Data sender.'))
    parser.add_argument(
        '--project_id', required=True, help='GCP cloud project name')
    parser.add_argument(
        '--registry_id', required=True, help='Cloud IoT Core registry id')
    parser.add_argument(
        '--device_id', required=True, help='Cloud IoT Core device id')
    parser.add_argument(
        '--private_key_file', required=True, help='Path to private key file.')
    parser.add_argument(
        '--algorithm',
        choices=('RS256', 'ES256'),
        required=True,
        help='The encryption algorithm to use to generate the JWT.')
    parser.add_argument(
        '--cloud_region', default='us-central1', help='GCP cloud region')
    parser.add_argument(
        '--ca_certs',
        default='roots.pem',
        help=('CA root from https://pki.google.com/roots.pem'))
    parser.add_argument(
        '--message_type',
        choices=('event', 'state'),
        default='event',
        required=True,
        help=('Indicates whether the message to be published is a '
              'telemetry event or a device state message.'))
    parser.add_argument(
        '--base_url',
        default=_BASE_URL,
        help=('Base URL for the Cloud IoT Core Device Service API'))
    parser.add_argument(
        '--jwt_expires_minutes',
        default=20,
        type=int,
        help=('Expiration time, in minutes, for JWT tokens.'))
    parser.add_argument(
        '--id',
        default=999,
        type=int,
        help=('Device id, not IoT Core device id for unique key.'))
    parser.add_argument(
        '--location_logitude',
        default=0.0,
        type=float,
        help=('Logitude of this deice. ex)35.658581'))
    parser.add_argument(
        '--location_latitude',
        default=0.0,
        type=float,
        help=('Latitude of this deice. ex)139.745433'))

    return parser.parse_args()


def write_ng_data(message_data):
    datas = json.loads(message_data)
    #送信に失敗した場合、send_ng_message.txtファイルに送れなかったメッセージを書き込む。
    ng_message_file = open('send_ng_message.txt', 'a')
    ng_message_file.write(str(datas['ID']))
    ng_message_file.write(',')
    ng_message_file.write(str(datas['LOCATION_LOGI']))
    ng_message_file.write(',')
    ng_message_file.write(str(datas['LOCATION_LATI']))
    ng_message_file.write(',')
    ng_message_file.write(str(datas['DEVICE_DATETIME']))
    ng_message_file.write(',')
    ng_message_file.write(str(datas['TEMPERATURE']))
    ng_message_file.write(',')
    ng_message_file.write(str(datas['PRESSURE']))
    ng_message_file.write(',')
    ng_message_file.write(str(datas['HUMIDITY']))

    ng_message_file.write('\n')
    ng_message_file.close()


def send_message(args, jwt_token, jwt_iat, jwt_exp_mins):
    seconds_since_issue = (datetime.datetime.utcnow() - jwt_iat).seconds
    if seconds_since_issue > 60 * jwt_exp_mins:
        #print('Refreshing token after {}s').format(seconds_since_issue)
        jwt_token = create_jwt(args.project_id, args.private_key_file,
                               args.algorithm)
        jwt_iat = datetime.datetime.utcnow()

    message_data = create_message(args.id, args.location_logitude,
                                  args.location_latitude)

    #print('Publishing message : \'{}\''.format(message_data))

    try:
        resp = publish_message(message_data, args.message_type, args.base_url,
                               args.project_id, args.cloud_region,
                               args.registry_id, args.device_id, jwt_token)
    except:
        resp = None

    #print(resp)
    #On HTTP error , write datas to csv file.
    if (resp is None) or (resp.status_code != requests.codes.ok):
        write_ng_data(message_data)


def main():
    args = parse_command_line_args()

    jwt_token = create_jwt(args.project_id, args.private_key_file,
                           args.algorithm)
    jwt_iat = datetime.datetime.utcnow()
    jwt_exp_mins = args.jwt_expires_minutes

    # Publish mesages to the HTTP bridge once per minite.
    while True:

        send_message(args, jwt_token, jwt_iat, jwt_exp_mins)

        #5分に一度データを送信する
        time.sleep(300 if args.message_type == 'event' else 5)
    #print('Finished.')


if __name__ == '__main__':
    main()
