수정 사항 Changelog

v 0.0.22
- EW11 TCP fallback에서 짧은 plug 상태 패킷이 들어와도 수신 루프가 오류 폭주하지 않도록 방어

v 0.0.21
- EW11 TCP fallback 활성화 시 헬스체크도 socket 수신 타이머를 보도록 수정

v 0.0.20
- MQTT mode에서도 EW11 TCP 수신 fallback을 기본 활성화해 ew11/recv MQTT가 멈춰도 상태 패킷을 직접 수신

v 0.0.19
- EW11/RS485 수신 헬스체크를 일반 MQTT 트래픽과 분리
- HA status/discovery 메시지만 들어오는 상태에서 수신 루프가 죽어도 자동 복구/재시작되도록 수정
- 헬스체크 회귀 테스트 추가

v 0.0.1
준비중
