/*
 * FFD MQTT
 */

#include <mosquitto.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <signal.h>
#include <direct.h>
#include "getopt.c"
#define fileno _fileno
#pragma warning(disable: 4996)

static unsigned int quit = 0;
static unsigned char logroot[200] = "D:\\SYSTEMLOG\\FFD_MQTT_SYSTEM_LOG.TXT";
static FILE* logfile = NULL;
static unsigned char dumpingroot[200] = "D:\\BIOLOG\\";
static unsigned char configroot[200] = "D:\\FFDCONFIG\\FFDMQTTCONFIG.INI";
static unsigned char* DUMPINGROOT_CUT;
static unsigned char* LOGROOT_CUT;
static struct stat statbuf;
static size_t len;
static unsigned int rewrite_switch = 0;
static FILE* file = NULL;
static unsigned char fileBuffer[1000]="";

typedef struct mqtt_string_t {
	size_t length;    /**< length of string */
	uint8_t* s;       /**< string data */
} mqtt_string_t;

typedef struct _DICTIONARY {
	mqtt_string_t id;
	mqtt_string_t topic;
	mqtt_string_t filename;
	mqtt_string_t fileroot;
	unsigned int toggle;
	unsigned int maxnum;
	unsigned int difnum;
	unsigned int pernum;
	unsigned int timer;
	unsigned int counter_stop;
	unsigned int restart_count;
	unsigned int restart_switch;
	unsigned int network_stat;
	FILE* file;
	time_t ptime;
	time_t ftime;
	struct tm* pinfo;
	struct tm* finfo;
	clock_t startc;
	clock_t endc;
	struct _DICTIONARY* link;
}_dictionary;

typedef struct DICTIONARY {
	unsigned int count;
	struct _DICTIONARY* head;
}dictionary;

static dictionary rfd;

static void MakeDirectory(unsigned char* full_path)
{
    unsigned char temp[256], * sp;
    strcpy(temp, full_path); // 경로문자열을 복사
    sp = temp; // 포인터를 문자열 처음으로

    while ((sp = strchr(sp, '\\'))) { // 디렉토리 구분자를 찾았으면
        if (sp > temp && *(sp - 1) != ':') { // 루트디렉토리가 아니면
            *sp = '\0'; // 잠시 문자열 끝으로 설정
            _mkdir(temp); // 디렉토리를 만들고 (존재하지 않을 때)
            * sp = '\\'; // 문자열을 원래대로 복귀
        }
        sp++; // 포인터를 다음 문자로 이동
    }

}

static void DICTIONARY_INIT(dictionary* head) {
    head->count = 0;
    head->head = NULL;
}

static unsigned int DICTIONARY_ADD(dictionary* head, unsigned char* id, unsigned char* topic, unsigned char* filename, unsigned int maxnum) {
    _dictionary* temp = head->head;
    unsigned char str[200], fileroot[200], * sp;
    time_t FILE_NAME_TIME;
    struct tm* FILE_NAME_TIME_TM;
    time(&FILE_NAME_TIME);
    FILE_NAME_TIME_TM = localtime(&FILE_NAME_TIME);
    strcpy(str, topic);
    sp = str;
    while (1) {
        // 헤더가 비었을 때
        if (head->count == 0/*temp == NULL*/) {
            temp = (_dictionary*)malloc(sizeof(_dictionary));
            //id
            temp->id.length = strlen(id);
            temp->id.s = (unsigned char*)malloc(temp->id.length + 1);
            memcpy(temp->id.s, id, temp->id.length + 1);
            //topic
            temp->topic.length = strlen(topic);
            temp->topic.s = (unsigned char*)malloc(temp->topic.length + 1);
            memcpy(temp->topic.s, topic, temp->topic.length + 1);
            //filename
            temp->filename.length = strlen(filename);
            temp->filename.s = (unsigned char*)malloc(temp->filename.length + 1);
            memcpy(temp->filename.s, filename, temp->filename.length + 1);

            temp->maxnum = maxnum;
            temp->pernum = 1;
            temp->counter_stop = 0;
            temp->restart_count = 0;
            temp->restart_switch = 0;
            temp->timer = 0;
            temp->network_stat = 0;

            while ((sp = strchr(sp, '/'))) {
                *sp = '\\';
                sp++; // 포인터를 다음 문자로 이동
            }

            //sprintf(fileroot, "%s%s\\%s", dumpingroot, str, filename);
            sprintf(fileroot, "%s%s\\%04d-%02d-%02d_%02d-%02d-%02d-%s", dumpingroot, str, FILE_NAME_TIME_TM->tm_year + 1900, FILE_NAME_TIME_TM->tm_mon + 1, FILE_NAME_TIME_TM->tm_mday, FILE_NAME_TIME_TM->tm_hour, FILE_NAME_TIME_TM->tm_min, FILE_NAME_TIME_TM->tm_sec, filename);
            //printf(fileroot);
            MakeDirectory(fileroot);

            //fileroot
            temp->fileroot.length = strlen(fileroot);
            temp->fileroot.s = (unsigned char*)malloc(temp->fileroot.length + 1);
            memcpy(temp->fileroot.s, fileroot, temp->fileroot.length + 1);

            temp->file = fopen((unsigned char*)temp->fileroot.s, "r");
            if (temp->file == NULL) {
                temp->toggle = 1;
                temp->file = fopen((unsigned char*)temp->fileroot.s, "a");
            }
            else {
                temp->toggle = 1;
                while (1) {
                    fgets(fileBuffer, sizeof(fileBuffer), temp->file);
                    temp->toggle++;
                    if (feof(temp->file)) { break; }
                }
                temp->file = freopen((unsigned char*)temp->fileroot.s, "a", temp->file);
                if (maxnum > temp->toggle) {
                    fputs("\n", temp->file);
                    fflush(temp->file);
                }
            }

            temp->link = NULL;
            head->head = temp;
            break;
        }

        // 현재 노드의 키가 새로 추가하려는 키와 같을 때
        else if (strcmp(temp->id.s, id) == 0) {
            return 0;
        }

        // 헤더(현재 노드)가 마지막 일때
        else if (temp->link == NULL) {
            temp->link = (_dictionary*)malloc(sizeof(_dictionary));
            //id
            temp->link->id.length = strlen(id);
            temp->link->id.s = (unsigned char*)malloc(temp->link->id.length + 1);
            memcpy(temp->link->id.s, id, temp->link->id.length + 1);
            //topic
            temp->link->topic.length = strlen(topic);
            temp->link->topic.s = (unsigned char*)malloc(temp->link->topic.length + 1);
            memcpy(temp->link->topic.s, topic, temp->link->topic.length + 1);
            //filename
            temp->link->filename.length = strlen(filename);
            temp->link->filename.s = (unsigned char*)malloc(temp->link->filename.length + 1);
            memcpy(temp->link->filename.s, filename, temp->link->filename.length + 1);

            temp->link->maxnum = maxnum;
            temp->link->pernum = 1;
            temp->link->counter_stop = 0;
            temp->link->restart_count = 0;
            temp->link->restart_switch = 0;
            temp->link->timer = 0;
            temp->link->network_stat = 0;

            while ((sp = strchr(sp, '/'))) {
                *sp = '\\';
                sp++; // 포인터를 다음 문자로 이동
            }

            //sprintf(fileroot, "%s%s\\%s", dumpingroot, str, filename);
            sprintf(fileroot, "%s%s\\%04d-%02d-%02d_%02d-%02d-%02d-%s", dumpingroot, str, FILE_NAME_TIME_TM->tm_year + 1900, FILE_NAME_TIME_TM->tm_mon + 1, FILE_NAME_TIME_TM->tm_mday, FILE_NAME_TIME_TM->tm_hour, FILE_NAME_TIME_TM->tm_min, FILE_NAME_TIME_TM->tm_sec, filename);
            //printf(fileroot);
            MakeDirectory(fileroot);

            //fileroot
            temp->link->fileroot.length = strlen(fileroot);
            temp->link->fileroot.s = (unsigned char*)malloc(temp->link->fileroot.length + 1);
            memcpy(temp->link->fileroot.s, fileroot, temp->link->fileroot.length + 1);

            temp->link->file = fopen((unsigned char*)temp->link->fileroot.s, "r");
            if (temp->link->file == NULL) {
                temp->link->toggle = 1;
                temp->link->file = fopen((unsigned char*)temp->link->fileroot.s, "a");
            }
            else {
                temp->link->toggle = 1;
                while (1) {
                    fgets(fileBuffer, sizeof(fileBuffer), temp->link->file);
                    temp->link->toggle++;
                    if (feof(temp->link->file)) { break; }
                }
                temp->link->file = freopen((unsigned char*)temp->link->fileroot.s, "a", temp->link->file);
                if (maxnum > temp->link->toggle) {
                    fputs("\n", temp->link->file);
                    fflush(temp->link->file);
                }
            }
            temp->link->link = NULL;
            break;
        }
        else {
            temp = temp->link;
        }
    }
    head->count++;
    return 1;
}

static _dictionary* DICTIONARY_SEARCH(dictionary* head, unsigned char* id) {
    _dictionary* temp = head->head;
    while (1) {
        if (temp == NULL) return NULL;

        if (strcmp(temp->id.s, id) == 0) return temp;

        temp = temp->link;

    }
}

static unsigned int DICTIONARY_MODIFY(dictionary* head, unsigned char* id, unsigned char* topic, unsigned char* filename, unsigned int maxnum) {
    _dictionary* temp = DICTIONARY_SEARCH(head, id);
    unsigned char str[200], fileroot[200], * sp;
    time_t FILE_NAME_TIME;
    struct tm* FILE_NAME_TIME_TM;
    time(&FILE_NAME_TIME);
    FILE_NAME_TIME_TM = localtime(&FILE_NAME_TIME);
    strcpy(str, topic);
    sp = str;
    if (temp == NULL) { return 0; }
    else {
        free(temp->id.s);
        free(temp->topic.s);
        free(temp->filename.s);
        free(temp->fileroot.s);
        fclose(temp->file);

        //id
        temp->id.length = strlen(id);
        temp->id.s = (unsigned char*)malloc(temp->id.length + 1);
        memcpy(temp->id.s, id, temp->id.length + 1);
        //topic
        temp->topic.length = strlen(topic);
        temp->topic.s = (unsigned char*)malloc(temp->topic.length + 1);
        memcpy(temp->topic.s, topic, temp->topic.length + 1);
        //filename
        temp->filename.length = strlen(filename);
        temp->filename.s = (unsigned char*)malloc(temp->filename.length + 1);
        memcpy(temp->filename.s, filename, temp->filename.length + 1);

        temp->maxnum = maxnum;
        temp->pernum = 1;
        temp->counter_stop = 0;
        temp->restart_count = 0;
        temp->restart_switch = 0;
        temp->timer = 0;
        temp->network_stat = 0;

        while ((sp = strchr(sp, '/'))) {
            *sp = '\\';
            sp++; // 포인터를 다음 문자로 이동
        }

        //sprintf(fileroot, "%s%s\\%s", dumpingroot, str, filename);
        sprintf(fileroot, "%s%s\\%04d-%02d-%02d_%02d-%02d-%02d-%s", dumpingroot, str, FILE_NAME_TIME_TM->tm_year + 1900, FILE_NAME_TIME_TM->tm_mon + 1, FILE_NAME_TIME_TM->tm_mday, FILE_NAME_TIME_TM->tm_hour, FILE_NAME_TIME_TM->tm_min, FILE_NAME_TIME_TM->tm_sec, filename);
        //printf(fileroot);
        MakeDirectory(fileroot);

        //fileroot
        temp->fileroot.length = strlen(fileroot);
        temp->fileroot.s = (unsigned char*)malloc(temp->fileroot.length + 1);
        memcpy(temp->fileroot.s, fileroot, temp->fileroot.length + 1);

        temp->file = fopen((unsigned char*)temp->fileroot.s, "r");
        if (temp->file == NULL) {
            temp->toggle = 1;
            temp->file = fopen((unsigned char*)temp->fileroot.s, "a");
        }
        else {
            temp->toggle = 1;
            while (1) {
                fgets(fileBuffer, sizeof(fileBuffer), temp->file);
                temp->toggle++;
                if (feof(temp->file)) { break; }
            }
            temp->file = freopen((unsigned char*)temp->fileroot.s, "a", temp->file);
            if (maxnum > temp->toggle) {
                fputs("\n", temp->file);
                fflush(temp->file);
            }
        }

        return 1;
    }
}

static unsigned int DICTIONARY_DELETE(dictionary* head, unsigned char* id) {

    _dictionary* ptr = head->head;
    _dictionary* pre = NULL;

    while (1) {
        if (ptr == NULL) return 0;
        else if (strcmp(ptr->id.s, id) == 0) break;

        pre = ptr;
        ptr = ptr->link;
    }

    if (pre == NULL) {
        free(ptr->id.s);
        free(ptr->topic.s);
        free(ptr->filename.s);
        free(ptr->fileroot.s);
        fclose(ptr->file);
        free(ptr);
        head->head = NULL;
    }
    else {
        pre->link = ptr->link;
        free(ptr->id.s);
        free(ptr->topic.s);
        free(ptr->filename.s);
        free(ptr->fileroot.s);
        fclose(ptr->file);
        free(ptr);
    }
    head->count--;
    return 1;
}

static void DICTIONARY_FREE(dictionary* head) {
    _dictionary* ptr = head->head;
    _dictionary* next = NULL;
    if (ptr == NULL) {
        return;
    }
    else {
        next = ptr->link;
        while (1) {
            free(ptr->id.s);
            free(ptr->topic.s);
            free(ptr->filename.s);
            free(ptr->fileroot.s);
            fclose(ptr->file);
            free(ptr);
            if (next == NULL) {
                break;
            }
            ptr = next;
            next = ptr->link;
        }
        head->count = 0;
        head->head = NULL;
        return;
    }
}

static void
handle_sigint(int signum) {
    quit = 1;
}

 /* Callback called when the client receives a CONNACK message from the broker. */
static void on_connect(struct mosquitto* mosq, void* obj, int reason_code)
{
	int rc;
	/* Print out the connection result. mosquitto_connack_string() produces an
	 * appropriate string for MQTT v3.x clients, the equivalent for MQTT v5.0
	 * clients is mosquitto_reason_string().
	 */
	printf("on_connect: %s\n", mosquitto_connack_string(reason_code));
	//if (reason_code != 0) {
		/* If the connection fails for any reason, we don't want to keep on
		 * retrying in this example, so disconnect. Without this, the client
		 * will attempt to reconnect. */
		//mosquitto_disconnect(mosq);
	//}

	/* Making subscriptions in the on_connect() callback means that if the
	 * connection drops and is automatically resumed by the client, then the
	 * subscriptions will be recreated when the client reconnects. */
    rc = mosquitto_subscribe(mosq, NULL, "COMMAND", 0);
	rc = mosquitto_subscribe(mosq, NULL, "FILE/#", 0);
	//if (rc != MOSQ_ERR_SUCCESS) {
		//fprintf(stderr, "Error subscribing: %s\n", mosquitto_strerror(rc));
		/* We might as well disconnect if we were unable to subscribe */
		//mosquitto_disconnect(mosq);
	//}
}


/* Callback called when the broker sends a SUBACK in response to a SUBSCRIBE. */
static void on_subscribe(struct mosquitto* mosq, void* obj, int mid, int qos_count, const int* granted_qos)
{
	int i;
	bool have_subscription = false;

	/* In this example we only subscribe to a single topic at once, but a
	 * SUBSCRIBE can contain many topics at once, so this is one way to check
	 * them all. */
	for (i = 0; i < qos_count; i++) {
		printf("on_subscribe: %d:granted qos = %d\n", i, granted_qos[i]);
		if (granted_qos[i] <= 2) {
			have_subscription = true;
		}
	}
	if (have_subscription == false) {
		/* The broker rejected all of our subscriptions, we know we only sent
		 * the one SUBSCRIBE, so there is no point remaining connected. */
		fprintf(stderr, "Error: All subscriptions rejected.\n");
		mosquitto_disconnect(mosq);
	}
}


/* Callback called when the client knows to the best of its abilities that a
 * PUBLISH has been successfully sent. For QoS 0 this means the message has
 * been completely written to the operating system. For QoS 1 this means we
 * have received a PUBACK from the broker. For QoS 2 this means we have
 * received a PUBCOMP from the broker. */
static void on_publish(struct mosquitto* mosq, void* obj, int mid)
{
	//printf("Message with mid %d has been published.\n", mid);
}


/* Callback called when the client receives a message. */
static void on_message(struct mosquitto* mosq, void* obj, const struct mosquitto_message* msg)
{
	/* This blindly prints the payload, but the payload can be anything so take care. */
	//printf("%s %d %s\n", msg->topic, msg->qos, (char*)msg->payload);
    unsigned char buf[200];
    unsigned char* cut;
    unsigned char* id;
    unsigned char* topic;
    unsigned char* filename;
    unsigned char* filedata;
    unsigned char* network_quality;
    unsigned char* network_signal_level;
    unsigned int maxnum;
    unsigned int result;
    unsigned int num;
    int rc;
    size_t len;
    _dictionary* rfdcli;

    if (strcmp(msg->topic, "COMMAND") == 0) {
        cut = strtok((char*)msg->payload, ":");
        if (strcmp(cut, "READY") == 0) {
            id = strtok(NULL, ":");
            topic = strtok(NULL, ":");
            filename = strtok(NULL, ":");
            maxnum = atoi(strtok(NULL, ":"));
            if (result = DICTIONARY_ADD(&rfd, id, topic, filename, maxnum) == 0) {
                rfdcli = DICTIONARY_SEARCH(&rfd, id);
                if (rfdcli == NULL) {
                    DICTIONARY_ADD(&rfd, id, topic, filename, maxnum);
                }
                else if (!((strcmp(rfdcli->topic.s, topic) == 0) && (strcmp(rfdcli->filename.s, filename) == 0) && (rfdcli->maxnum == maxnum))) {
                    DICTIONARY_MODIFY(&rfd, id, topic, filename, maxnum);
                }
            }
            rfdcli = DICTIONARY_SEARCH(&rfd, id);
            if (rfdcli == NULL) {
                printf("ID : %s, TOPIC : %s, FILE : %s 구조체 생성 오류\n", id, topic, filename);
                quit = 1;
                len = sprintf((unsigned char*)buf, "ERR:M");
                rc = mosquitto_publish(mosq, NULL, id, strlen(buf), buf, 0, false);
                if (rc != MOSQ_ERR_SUCCESS) {
                    fprintf(stderr, "Error publishing: %s\n", mosquitto_strerror(rc));
                }
                return;
            }
            if (rfdcli->toggle == maxnum) {
                printf("ID : %s, TOPIC : %s, FILE : %s 의 파일을 이전에 이미 다 받음\n", id, topic, filename);
                len = sprintf((unsigned char*)buf, "DELETE:E");
                rc = mosquitto_publish(mosq, NULL, id, strlen(buf), buf, 0, false);
                if (rc != MOSQ_ERR_SUCCESS) {
                    fprintf(stderr, "Error publishing: %s\n", mosquitto_strerror(rc));
                }
                return;
            }
            if (rfdcli->toggle > maxnum) {
                printf("ID : %s, TOPIC : %s, FILE : %s 의 파일이 기존에 받았던 파일과 다르거나 초과로 받음(전송전 오류)\n", id, topic, filename);
                len = sprintf((unsigned char*)buf, "ERR:F");
                rc = mosquitto_publish(mosq, NULL, id, strlen(buf), buf, 0, false);
                if (rc != MOSQ_ERR_SUCCESS) {
                    fprintf(stderr, "Error publishing: %s\n", mosquitto_strerror(rc));
                }
                return;
            }
            rfdcli->network_stat = 1;
            len = sprintf((unsigned char*)buf, "OK:%d", rfdcli->toggle);
            rc = mosquitto_publish(mosq, NULL, id, strlen(buf), buf, 0, false);
            if (rc != MOSQ_ERR_SUCCESS) {
                fprintf(stderr, "Error publishing: %s\n", mosquitto_strerror(rc));
            }
            return;
        }
        else if (strcmp(cut, "READYANDSTAT") == 0) {
            id = strtok(NULL, ":");
            topic = strtok(NULL, ":");
            filename = strtok(NULL, ":");
            maxnum = atoi(strtok(NULL, ":"));
            network_quality = strtok(NULL, ":");
            network_signal_level = strtok(NULL, ":");
            if (result = DICTIONARY_ADD(&rfd, id, topic, filename, maxnum) == 0) {
                rfdcli = DICTIONARY_SEARCH(&rfd, id);
                if (rfdcli == NULL) {
                    DICTIONARY_ADD(&rfd, id, topic, filename, maxnum);
                }
                else if (!((strcmp(rfdcli->topic.s, topic) == 0) && (strcmp(rfdcli->filename.s, filename) == 0) && (rfdcli->maxnum == maxnum))) {
                    DICTIONARY_MODIFY(&rfd, id, topic, filename, maxnum);
                }
            }
            rfdcli = DICTIONARY_SEARCH(&rfd, id);
            if (rfdcli == NULL) {
                printf("ID : %s, TOPIC : %s, FILE : %s 구조체 생성 오류\n", id, topic, filename);
                quit = 1;
                len = sprintf((unsigned char*)buf, "ERR:M");
                rc = mosquitto_publish(mosq, NULL, id, strlen(buf), buf, 0, false);
                if (rc != MOSQ_ERR_SUCCESS) {
                    fprintf(stderr, "Error publishing: %s\n", mosquitto_strerror(rc));
                }
                return;
            }
            if (rfdcli->toggle == maxnum) {
                printf("ID : %s, TOPIC : %s, FILE : %s 의 파일을 이전에 이미 다 받음\n", id, topic, filename);
                len = sprintf((unsigned char*)buf, "DELETE:E");
                rc = mosquitto_publish(mosq, NULL, id, strlen(buf), buf, 0, false);
                if (rc != MOSQ_ERR_SUCCESS) {
                    fprintf(stderr, "Error publishing: %s\n", mosquitto_strerror(rc));
                }
                return;
            }
            if (rfdcli->toggle > maxnum) {
                printf("ID : %s, TOPIC : %s, FILE : %s 의 파일이 기존에 받았던 파일과 다르거나 초과로 받음(전송전 오류)\n", id, topic, filename);
                len = sprintf((unsigned char*)buf, "ERR:F");
                rc = mosquitto_publish(mosq, NULL, id, strlen(buf), buf, 0, false);
                if (rc != MOSQ_ERR_SUCCESS) {
                    fprintf(stderr, "Error publishing: %s\n", mosquitto_strerror(rc));
                }
                return;
            }
            if (!(rfdcli->network_stat)) {
                logfile = fopen(logroot, "a");

                sprintf(buf, "ID : %s, TOPIC : %s, FILENAME : %s, MAXNUM : %d, NETWORK QUALITY : %s, NETWORK SIGNAL LEVEL : %s dBm\n", id, topic, filename, maxnum, network_quality, network_signal_level);
                printf(buf);
                fputs(buf, logfile);

                fflush(logfile);
                fclose(logfile);
            }
            rfdcli->network_stat = 1;
            len = sprintf((unsigned char*)buf, "OK:%d", rfdcli->toggle);
            rc = mosquitto_publish(mosq, NULL, id, strlen(buf), buf, 0, false);
            if (rc != MOSQ_ERR_SUCCESS) {
                fprintf(stderr, "Error publishing: %s\n", mosquitto_strerror(rc));
            }
            return;
        }
        else if (strcmp(cut, "OK") == 0) {
            id = strtok(NULL, ":");
            rfdcli = DICTIONARY_SEARCH(&rfd, id);
            if (rfdcli == NULL) {
                len = sprintf((unsigned char*)buf, "RESTART:0");
                rc = mosquitto_publish(mosq, NULL, id, strlen(buf), buf, 0, false);
                if (rc != MOSQ_ERR_SUCCESS) {
                    fprintf(stderr, "Error publishing: %s\n", mosquitto_strerror(rc));
                }
                return;
            }
            len = sprintf((unsigned char*)buf, "RESTART:%d", rfdcli->toggle);
            rc = mosquitto_publish(mosq, NULL, id, strlen(buf), buf, 0, false);
            if (rc != MOSQ_ERR_SUCCESS) {
                fprintf(stderr, "Error publishing: %s\n", mosquitto_strerror(rc));
            }
            return;
        }
        else if (strcmp(cut, "EMPTY") == 0) {
            id = strtok(NULL, ":");
            len = sprintf((unsigned char*)buf, "ACK:");
            rc = mosquitto_publish(mosq, NULL, id, strlen(buf), buf, 0, false);
            if (rc != MOSQ_ERR_SUCCESS) {
                fprintf(stderr, "Error publishing: %s\n", mosquitto_strerror(rc));
            }
            return;
        }
        else if (strcmp(cut, "DONE") == 0) {
            id = strtok(NULL, ":");
            num = atoi(strtok(NULL, ":"));
            rfdcli = DICTIONARY_SEARCH(&rfd, id);
            if (rfdcli == NULL) {
                len = sprintf((unsigned char*)buf, "RESTART:0");
                rc = mosquitto_publish(mosq, NULL, id, strlen(buf), buf, 0, false);
                if (rc != MOSQ_ERR_SUCCESS) {
                    fprintf(stderr, "Error publishing: %s\n", mosquitto_strerror(rc));
                }
                return;
            }
            else if (rfdcli->toggle == num) {
                rfdcli->endc = clock();
                time(&(rfdcli->ftime));
                rfdcli->finfo = localtime(&(rfdcli->ftime));
                if (!(rfdcli->counter_stop)) {
                    logfile = fopen(logroot, "a");

                    sprintf(buf, "%s : %s : %s END : %s", rfdcli->id.s, rfdcli->topic.s, rfdcli->filename.s, asctime(rfdcli->finfo));
                    printf(buf);
                    fputs(buf, logfile);

                    sprintf(buf, "%s : %s : %s TOTAL : %lf, RESTART : %d\n", rfdcli->id.s, rfdcli->topic.s, rfdcli->filename.s, (double)(rfdcli->endc - rfdcli->startc) / CLOCKS_PER_SEC, rfdcli->restart_count);
                    printf(buf);
                    fputs(buf, logfile);
                    fflush(logfile);
                    fclose(logfile);
                    rfdcli->counter_stop = 1;
                }
                fclose(rfdcli->file);
                len = sprintf((unsigned char*)buf, "DELETE:N");
                rc = mosquitto_publish(mosq, NULL, id, strlen(buf), buf, 0, false);
                if (rc != MOSQ_ERR_SUCCESS) {
                    fprintf(stderr, "Error publishing: %s\n", mosquitto_strerror(rc));
                }
                return;
            }
            else if (rfdcli->toggle > num) {
                rfdcli->endc = clock();
                time(&(rfdcli->ftime));
                rfdcli->finfo = localtime(&(rfdcli->ftime));
                if (!(rfdcli->counter_stop)) {
                    logfile = fopen(logroot, "a");

                    sprintf(buf, "ID : %s, TOPIC : %s, FILE : %s 의 파일이 기존에 받았던 파일과 다르거나 초과로 받음(전송후 오류)", rfdcli->id.s, rfdcli->topic.s, rfdcli->filename.s);
                    printf(buf);
                    fputs(buf, logfile);

                    sprintf(buf, "%s : %s : %s OVER END : %s", rfdcli->id.s, rfdcli->topic.s, rfdcli->filename.s, asctime(rfdcli->finfo));
                    printf(buf);
                    fputs(buf, logfile);

                    sprintf(buf, "%s : %s : %s TOTAL : %lf, RESTART : %d\n", rfdcli->id.s, rfdcli->topic.s, rfdcli->filename.s, (double)(rfdcli->endc - rfdcli->startc) / CLOCKS_PER_SEC, rfdcli->restart_count);
                    printf(buf);
                    fputs(buf, logfile);
                    fflush(logfile);
                    fclose(logfile);
                    rfdcli->counter_stop = 1;
                }
                fclose(rfdcli->file);
                len = sprintf((unsigned char*)buf, "ERR:D");
                rc = mosquitto_publish(mosq, NULL, id, strlen(buf), buf, 0, false);
                if (rc != MOSQ_ERR_SUCCESS) {
                    fprintf(stderr, "Error publishing: %s\n", mosquitto_strerror(rc));
                }
                return;
            }
            else {
                rfdcli->restart_switch = 1;
                len = sprintf((unsigned char*)buf, "RESTART:%d", rfdcli->toggle);
                rc = mosquitto_publish(mosq, NULL, id, strlen(buf), buf, 0, false);
                if (rc != MOSQ_ERR_SUCCESS) {
                    fprintf(stderr, "Error publishing: %s\n", mosquitto_strerror(rc));
                }
                return;
            }
        }
        else if (strcmp(cut, "EXIT") == 0) {
            quit = 1;
            len = sprintf((unsigned char*)buf, "ACK:");
            rc = mosquitto_publish(mosq, NULL, "ALL", strlen(buf), buf, 0, false);
            if (rc != MOSQ_ERR_SUCCESS) {
                fprintf(stderr, "Error publishing: %s\n", mosquitto_strerror(rc));
            }
            return;
        }
    }
    else {
        cut = strtok(msg->topic, "/");
        if (strcmp(cut, "FILE") == 0) {
            topic = strtok(NULL, "/");
            id = strtok((char*)msg->payload, ":");
            num = atoi(strtok(NULL, ":"));
            filedata = strtok(NULL, ":");
            rfdcli = DICTIONARY_SEARCH(&rfd, id);
            if (rfdcli == NULL) {
                /*
                len = sprintf((unsigned char*)buf, "RESTART:0");
                rc = mosquitto_publish(mosq, NULL, id, strlen(buf), buf, 0, false);
                if (rc != MOSQ_ERR_SUCCESS) {
                    fprintf(stderr, "Error publishing: %s\n", mosquitto_strerror(rc));
                }
                */
                return;
            }
            else if (rfdcli->toggle != num) {
                /*
                if (rfdcli->toggle > rfdcli->maxnum) {
                    rfdcli->endc = clock();
                    time(&(rfdcli->ftime));
                    rfdcli->finfo = localtime(&(rfdcli->ftime));
                    if (!(rfdcli->counter_stop)) {
                        logfile = fopen(logroot, "a");

                        sprintf(buf, "ID : %s, TOPIC : %s, FILE : %s 의 파일이 기존에 받았던 파일과 다르거나 초과로 받음(전송중 오류)", rfdcli->id.s, rfdcli->topic.s, rfdcli->filename.s);
                        printf(buf);
                        fputs(buf, logfile);

                        sprintf(buf, "%s : %s : %s OVER END : %s", rfdcli->id.s, rfdcli->topic.s, rfdcli->filename.s, asctime(rfdcli->finfo));
                        printf(buf);
                        fputs(buf, logfile);

                        sprintf(buf, "%s : %s : %s TOTAL : %lf, RESTART : %d\n", rfdcli->id.s, rfdcli->topic.s, rfdcli->filename.s, (double)(rfdcli->endc - rfdcli->startc) / CLOCKS_PER_SEC, rfdcli->restart_count);
                        printf(buf);
                        fputs(buf, logfile);
                        fflush(logfile);
                        fclose(logfile);
                        rfdcli->counter_stop = 1;
                    }
                    fclose(rfdcli->file);
                    len = sprintf((unsigned char*)buf, "ERR:P");
                    rc = mosquitto_publish(mosq, NULL, id, strlen(buf), buf, 0, false);
                    if (rc != MOSQ_ERR_SUCCESS) {
                        fprintf(stderr, "Error publishing: %s\n", mosquitto_strerror(rc));
                    }
                    return;
                }
                len = sprintf((unsigned char*)buf, "STOP:");
                rfdcli->restart_switch = 1;
                rc = mosquitto_publish(mosq, NULL, id, strlen(buf), buf, 0, false);
                if (rc != MOSQ_ERR_SUCCESS) {
                    fprintf(stderr, "Error publishing: %s\n", mosquitto_strerror(rc));
                }
                */
                return;
            }
            else {
                fputs(filedata, rfdcli->file);
                fflush(rfdcli->file);
                if (!(rfdcli->timer) && !(rfdcli->counter_stop)) {
                    rfdcli->startc = clock();
                    time(&(rfdcli->ptime));
                    rfdcli->pinfo = localtime(&(rfdcli->ptime));
                    logfile = fopen(logroot, "a");
                    sprintf(buf, "%s : %s : %s START : %s", rfdcli->id.s, rfdcli->topic.s, rfdcli->filename.s, asctime(rfdcli->pinfo));
                    printf(buf);
                    fputs(buf, logfile);
                    fflush(logfile);
                    fclose(logfile);
                    rfdcli->difnum = (rfdcli->maxnum * rfdcli->pernum) / 10;
                    rfdcli->timer = 1;
                }
                else if (rfdcli->pernum < 10 && rfdcli->difnum == rfdcli->toggle) {
                    printf("%s : %s : %s DOWNLOAD : %d%%(%d line)\n", rfdcli->id.s, rfdcli->topic.s, rfdcli->filename.s, rfdcli->pernum * 10, rfdcli->difnum);
                    rfdcli->pernum++;
                    rfdcli->difnum = (rfdcli->maxnum * rfdcli->pernum) / 10;
                }
                rfdcli->toggle++;
                if (rfdcli->restart_switch) {
                    rfdcli->restart_count++;
                    rfdcli->restart_switch = 0;
                }
                return;
            }
        }
    }
}


int main(int argc, char** argv)
{
	struct mosquitto* mosq;
	int rc, opt;

    MakeDirectory(configroot);

    file = fopen(configroot, "r");
    if (file == NULL) {
        rewrite_switch = 1;
    }
    else {
        if (fstat(fileno(file), &statbuf) < 0) {
            rewrite_switch = 1;
        }
        else {
            len = fread(fileBuffer, 1, statbuf.st_size, file);
            if (len < 0) {
                rewrite_switch = 1;
            }
            else {
                DUMPINGROOT_CUT = strtok(fileBuffer, "\n");
                LOGROOT_CUT = strtok(NULL, "\n");
                if ((DUMPINGROOT_CUT != NULL) && (LOGROOT_CUT != NULL) && ((DUMPINGROOT_CUT = strtok(DUMPINGROOT_CUT, "=")) != NULL) && (strcmp(DUMPINGROOT_CUT, "DUMPINGROOT") == 0) && ((DUMPINGROOT_CUT = strtok(NULL, "=")) != NULL) && ((LOGROOT_CUT = strtok(LOGROOT_CUT, "=")) != NULL) && (strcmp(LOGROOT_CUT, "LOGROOT") == 0) && ((LOGROOT_CUT = strtok(NULL, "=")) != NULL)) {
                    sprintf(dumpingroot, DUMPINGROOT_CUT);
                    sprintf(logroot, LOGROOT_CUT);
                }
                else {
                    rewrite_switch = 1;
                }
            }
        }
        fclose(file);
    }

    if (rewrite_switch) {
        file = fopen(configroot, "w");
        sprintf(fileBuffer, "DUMPINGROOT=D:\\BIOLOG\\\nLOGROOT=D:\\SYSTEMLOG\\FFD_MQTT_SYSTEM_LOG.TXT\n\n\n\n DUMPINGROOT(FFD가 RFD로부터 전송받는 파일들을 저장할 루트 폴더 경로)\n LOGROOT(FFD가 로그 파일을 저장할 전체 경로)\n 순으로 변수명뒤에 띄어쓰기 없이 =과 텍스트를 붙임으로서 설정할 수 있으며 줄바꿈으로 값을 구별합니다.\n");
        fputs(fileBuffer, file);
        fflush(file);
        fclose(file);
    }
    rewrite_switch = 0;

    while ((opt = getopt(argc, argv, "a:D:")) != -1) {
        switch (opt) {
        case 'a':
            sprintf(dumpingroot, optarg);
            rewrite_switch = 1;
            break;
        case 'D':
            sprintf(logroot, optarg);
            rewrite_switch = 1;
            break;
        default:
            printf("잘못된 명령어 인자 입력\n");
            exit(1);
        }
    }

    signal(SIGINT, handle_sigint);

    if (rewrite_switch) {
        file = fopen(configroot, "w");
        sprintf(fileBuffer, "DUMPINGROOT=%s\nLOGROOT=%s\n\n\n\n DUMPINGROOT(FFD가 RFD로부터 전송받는 파일들을 저장할 루트 폴더 경로)\n LOGROOT(FFD가 로그 파일을 저장할 전체 경로)\n 순으로 변수명뒤에 띄어쓰기 없이 =과 텍스트를 붙임으로서 설정할 수 있으며 줄바꿈으로 값을 구별합니다.\n", dumpingroot, logroot);
        fputs(fileBuffer, file);
        fflush(file);
        fclose(file);
    }

	/* Required before calling other mosquitto functions */
	mosquitto_lib_init();

	/* Create a new client instance.
	 * id = NULL -> ask the broker to generate a client id for us
	 * clean session = true -> the broker should remove old sessions when we connect
	 * obj = NULL -> we aren't passing any of our private data for callbacks
	 */
	mosq = mosquitto_new(NULL, true, NULL);
	if (mosq == NULL) {
		fprintf(stderr, "Error: Out of memory.\n");
		return 1;
	}

	/* Configure callbacks. This should be done before connecting ideally. */
	mosquitto_connect_callback_set(mosq, on_connect);
	mosquitto_subscribe_callback_set(mosq, on_subscribe);
	mosquitto_publish_callback_set(mosq, on_publish);
	mosquitto_message_callback_set(mosq, on_message);

	/* Connect to test.mosquitto.org on port 1883, with a keepalive of 60 seconds.
	 * This call makes the socket connection only, it does not complete the MQTT
	 * CONNECT/CONNACK flow, you should use mosquitto_loop_start() or
	 * mosquitto_loop_forever() for processing net traffic. */
	rc = mosquitto_connect(mosq, "localhost", 1883, 60);
	if (rc != MOSQ_ERR_SUCCESS) {
		mosquitto_destroy(mosq);
		fprintf(stderr, "Error: %s\n", mosquitto_strerror(rc));
		return 1;
	}

    MakeDirectory(logroot);
    MakeDirectory(dumpingroot);

    DICTIONARY_INIT(&rfd);

	/* Run the network loop in a blocking call. The only thing we do in this
	 * example is to print incoming messages, so a blocking call here is fine.
	 *
	 * This call will continue forever, carrying automatic reconnections if
	 * necessary, until the user calls mosquitto_disconnect().
	 */
    while (!quit) {
        mosquitto_loop(mosq, -1, 1);
    }

    printf("MQTT 종료\n");

    DICTIONARY_FREE(&rfd);

	mosquitto_lib_cleanup();
	return 0;
}

