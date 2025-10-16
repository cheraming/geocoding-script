import pandas as pd
import requests
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import json
import threading

# 카카오 REST API 키
KAKAO_API_KEY = "6180854cc949ae0eb09abee0fb566c2b"
headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}

# 기본 지역
BASE_REGION = "강원특별자치도 양구군 양구읍"
REGION_BOUNDS = {  # 양구군 대략적 경계
    'min_lat': 37.8, 'max_lat': 38.3,
    'min_lng': 127.8, 'max_lng': 128.3
}

# 성능 설정
MAX_WORKERS = 6  # 정확도를 위해 스레드 수 약간 감소
BATCH_SIZE = 30

class RateLimiter:
    def __init__(self, max_calls_per_second=15):  # API 제한을 위해 약간 보수적으로
        self.max_calls_per_second = max_calls_per_second
        self.calls = []
        self.lock = threading.Lock()
    
    def wait_if_needed(self):
        with self.lock:
            now = time.time()
            self.calls = [call_time for call_time in self.calls if now - call_time < 1.0]
            
            if len(self.calls) >= self.max_calls_per_second:
                sleep_time = 1.0 - (now - self.calls[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
            
            self.calls.append(now)

rate_limiter = RateLimiter()

# 캐싱
address_cache = {}

def clean_address(addr):
    if not addr or pd.isna(addr):
        return ""
    
    addr_str = str(addr).strip()
    
    if addr_str in address_cache:
        return address_cache[addr_str]
    
    # 정규식 패턴 (한 번만 컴파일)
    if not hasattr(clean_address, 'patterns'):
        clean_address.patterns = {
            'brackets': re.compile(r'\([^)]*\)|\[[^\]]*\]|\{[^}]*\}|【[^】]*】|「[^」]*」'),
            'floors': re.compile(r'\d+층\s*|지하\d*층?\s*|B\d*F?\s*|\d+F\s*|\d+호\s*|지하\s*|옥상\s*|루프탑\s*', re.IGNORECASE),
            'business': re.compile(r'[가-힣]+(?:점|관|원|실|센터|마트|마켓|상회|상점|업체|회사|공사|건설|부동산|약국|병원|의원|치과|한의원|식당|카페|커피|PC방|노래방|당구|볼링|찜질방|사우나|모텔|여관|펜션|민박)(?:\s|$)', re.IGNORECASE),
            'locations': re.compile(r'(?:앞|뒤|옆|좌|우|좌측|우측|왼쪽|오른쪽|가운데|중앙|정면|측면|입구|출구|근처|사이|건너편|첫번째|두번째|세번째|네번째|다섯번째|\d+번째)(?:\s|$)', re.IGNORECASE),
            'detail_addr': re.compile(r'(\d+)-\d+'),
            'special_chars': re.compile(r'[~!@#$%^&*+=|\\:";\'<>?/,.-]'),
            'spaces': re.compile(r'\s+')
        }
    
    patterns = clean_address.patterns
    addr = patterns['brackets'].sub('', addr_str)
    addr = patterns['floors'].sub('', addr)
    addr = patterns['business'].sub(' ', addr)
    addr = patterns['locations'].sub(' ', addr)
    addr = patterns['detail_addr'].sub(r'\1', addr)
    addr = patterns['special_chars'].sub(' ', addr)
    addr = patterns['spaces'].sub(' ', addr).strip()
    
    address_cache[addr_str] = addr
    return addr

def is_valid_coordinate(lat, lng):
    """좌표가 양구군 범위 내에 있는지 확인"""
    return (REGION_BOUNDS['min_lat'] <= lat <= REGION_BOUNDS['max_lat'] and 
            REGION_BOUNDS['min_lng'] <= lng <= REGION_BOUNDS['max_lng'])

def validate_result_with_original(result_address, original_address):
    """검색 결과가 원본 주소와 유사한지 확인"""
    if not result_address or not original_address:
        return False
    
    # 핵심 키워드 추출
    original_words = set(re.findall(r'[가-힣]+', str(original_address)))
    result_words = set(re.findall(r'[가-힣]+', str(result_address)))
    
    # 공통 단어가 있는지 확인 (최소 1개 이상)
    common_words = original_words.intersection(result_words)
    return len(common_words) > 0

def make_api_request_with_validation(url, params, original_addr, timeout=10):
    """검증 로직이 포함된 API 요청"""
    rate_limiter.wait_if_needed()
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=timeout)
        
        if response.status_code == 401:
            return None, "API 키 오류"
        elif response.status_code == 429:
            time.sleep(0.2)
            return None, "API 제한"
        elif response.status_code == 200:
            result = response.json()
            
            if result.get("documents"):
                # 여러 결과가 있다면 가장 적절한 것 선택
                for doc in result["documents"][:3]:  # 상위 3개 검토
                    lat, lng = float(doc["y"]), float(doc["x"])
                    
                    # 1차: 좌표 범위 확인
                    if not is_valid_coordinate(lat, lng):
                        continue
                    
                    # 2차: 주소 유사성 확인 (키워드 검색에서만)
                    if 'keyword' in url:
                        result_addr = doc.get("place_name", "") + " " + doc.get("address_name", "")
                        if not validate_result_with_original(result_addr, original_addr):
                            continue
                    
                    return (lat, lng), f"검증완료"
                
                # 검증 통과한 결과가 없으면 첫 번째 결과라도 반환 (좌표 범위 내라면)
                first_doc = result["documents"][0]
                lat, lng = float(first_doc["y"]), float(first_doc["x"])
                if is_valid_coordinate(lat, lng):
                    return (lat, lng), "기본결과"
        
        return None, f"HTTP {response.status_code}"
        
    except Exception as e:
        return None, str(e)[:50]

def get_coords_with_accuracy_focus(address):
    """정확도에 중점을 둔 좌표 변환"""
    
    original_addr = str(address).strip()
    cleaned_addr = clean_address(original_addr)
    
    # 우선순위 기반 검색 패턴 (정확도 순)
    search_patterns = []
    
    if cleaned_addr:
        # 1순위: 가장 구체적인 주소
        search_patterns.append(f"{BASE_REGION} {cleaned_addr}")
        
        # 2순위: 도로명 추출
        road_match = re.search(r'([가-힣]+길|[가-힣]+로)', cleaned_addr)
        if road_match:
            road_name = road_match.group(1)
            search_patterns.append(f"{BASE_REGION} {road_name}")
        
        # 3순위: 점진적 축약 (하지만 너무 짧아지지 않도록)
        words = cleaned_addr.split()
        for i in range(len(words), max(1, len(words)-2), -1):  # 최대 2개 단어까지만 제거
            shorter = ' '.join(words[:i])
            if len(shorter) > 4:  # 최소 4글자 이상
                search_patterns.append(f"{BASE_REGION} {shorter}")
        
        # 4순위: 다른 지역명 표기
        search_patterns.extend([
            f"강원도 양구군 양구읍 {cleaned_addr}",
            f"양구읍 {cleaned_addr}",
        ])
    
    # 중복 제거 및 최대 6개로 제한 (속도와 정확도 균형)
    search_patterns = list(dict.fromkeys(search_patterns))[:6]
    
    # 각 패턴으로 검색 (검증 포함)
    for i, pattern in enumerate(search_patterns, 1):
        if not pattern.strip():
            continue
        
        # 주소 검색 (더 정확함)
        coords, status = make_api_request_with_validation(
            "https://dapi.kakao.com/v2/local/search/address.json",
            {"query": pattern},
            original_addr
        )
        
        if coords:
            return coords[0], coords[1], f"주소검색-{i}단계({status})"
        
        # 키워드 검색 (보조적)
        coords, status = make_api_request_with_validation(
            "https://dapi.kakao.com/v2/local/search/keyword.json",
            {"query": pattern, "size": 5},  # 더 많은 결과 검토
            original_addr
        )
        
        if coords:
            return coords[0], coords[1], f"키워드검색-{i}단계({status})"
    
    return None, None, "검증실패"

def process_address_batch(address_batch):
    """배치 처리"""
    results = []
    
    for idx, addr in address_batch:
        if pd.isna(addr) or not str(addr).strip():
            results.append((idx, None, None, "빈 주소"))
        else:
            lat, lng, method = get_coords_with_accuracy_focus(addr)
            results.append((idx, lat, lng, method))
    
    return results

def test_api_key():
    try:
        response = requests.get(
            "https://dapi.kakao.com/v2/local/search/address.json",
            headers=headers,
            params={"query": "서울특별시 강남구"},
            timeout=5
        )
        return response.status_code == 200
    except:
        return False

def main():
    print("🎯 정확도 중심 주소 좌표 변환 시작")
    start_time = time.time()
    
    # API 키 확인
    if not test_api_key():
        print("❌ API 키를 확인해주세요!")
        return
    print("✅ API 키 정상")
    
    # 엑셀 불러오기
    input_file = r"C:\소규모 관리블럭\데이터 입력\방산2 소블록 list.xlsx"
    try:
        df = pd.read_excel(input_file)
        print(f"📊 {len(df)}개 주소 로드 완료")
    except Exception as e:
        print(f"❌ 엑셀 파일 읽기 실패: {e}")
        return
    
    if "주소" not in df.columns:
        print("❌ 엑셀에 '주소' 열이 없습니다.")
        return
    
    # 결과 저장용
    latitudes = [None] * len(df)
    longitudes = [None] * len(df)
    methods = [None] * len(df)
    
    # 주소를 배치로 나누기
    address_data = [(idx, addr) for idx, addr in enumerate(df["주소"])]
    batches = [address_data[i:i+BATCH_SIZE] for i in range(0, len(address_data), BATCH_SIZE)]
    
    success_count = 0
    processed_count = 0
    
    print(f"🔍 {len(batches)}개 배치, {MAX_WORKERS}개 스레드로 정확도 중심 처리...")
    
    # 병렬 처리
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_batch = {executor.submit(process_address_batch, batch): batch for batch in batches}
        
        for future in as_completed(future_to_batch):
            batch_results = future.result()
            
            for idx, lat, lng, method in batch_results:
                latitudes[idx] = lat
                longitudes[idx] = lng
                methods[idx] = method
                
                if lat is not None:
                    success_count += 1
                processed_count += 1
            
            progress = (processed_count / len(df)) * 100
            print(f"진행률: {progress:.1f}% ({processed_count}/{len(df)}) | 성공: {success_count}")
            
            # 중간 저장
            if processed_count % (BATCH_SIZE * 3) == 0:
                temp_df = df.copy()
                temp_df["위도"] = latitudes
                temp_df["경도"] = longitudes
                temp_df["변환방법"] = methods
                temp_file = r"C:\소규모 관리블럭\결과값\주소_좌표변환_정확도중심_임시.xlsx"
                temp_df.to_excel(temp_file, index=False)
                print(f"💾 중간 저장 완료")
    
    # 결과 저장
    df["위도"] = latitudes
    df["경도"] = longitudes
    df["변환방법"] = methods
    
    output_file = r"C:\소규모 관리블럭\결과값\주소_좌표변환_정확도중심_방산2.xlsx"
    df.to_excel(output_file, index=False)
    
    # 처리 시간 계산
    end_time = time.time()
    total_time = end_time - start_time
    
    # 최종 결과
    fail_count = len(df) - success_count
    print(f"\n🎯 정확도 중심 변환 완료!")
    print(f"⏱️  총 처리 시간: {total_time:.1f}초")
    print(f"📊 최종 결과:")
    print(f"  - 총 처리: {len(df)}건")
    print(f"  - 성공: {success_count}건 ({success_count/len(df)*100:.1f}%)")
    print(f"  - 실패: {fail_count}건 ({fail_count/len(df)*100:.1f}%)")
    print(f"  - 처리 속도: {len(df)/total_time:.1f}건/초")
    print(f"📁 최종 파일: {output_file}")
    
    # 검증된 결과 분석
    verified_results = df[df["변환방법"].str.contains("검증완료", na=False)]
    print(f"🔍 검증 통과: {len(verified_results)}건 ({len(verified_results)/len(df)*100:.1f}%)")

if __name__ == "__main__":
    main()