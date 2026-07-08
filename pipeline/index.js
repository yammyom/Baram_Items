require('dotenv').config();
const axios = require('axios');
const cheerio = require('cheerio');
const pLimit = require('p-limit');
const { createClient } = require('@supabase/supabase-js');

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY);
const NEXON_API_KEY = process.env.NEXON_API_KEY;

const NEXON_SERVERS = { '연': 131073, '무휼': 131074, '유리': 131086, '하자': 131087, '호동': 131088, '진': 131089 };
const DB_SERVER_IDS = { '연': 1, '무휼': 2, '유리': 3, '하자': 4, '호동': 5, '진': 6 };
const JOBS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];

// 목표 승급 단계 범위 (하향 N차 ~ 상향 M차)
const MIN_PROMOTION_LEVEL = 7; // 하향 승급
const MAX_PROMOTION_LEVEL = 9; // 상향 승급

const PART_MAP = {
  '무기': 1, '투구': 2, '갑옷': 3, '왼손': 4, '오른손': 4,
  '목장식': 5, '목/어깨장식': 5, '신발': 6, '망토': 7, '얼굴장식': 8,
  '보조1': 9, '보조2': 9, '보조': 9, '장신구': 10, '세트옷': 11, '방패/보조무기': 12,
  '캐시 무기': 13, '캐시무기': 13,
  '캐시 투구': 14, '캐시투구': 14,
  '캐시 겉옷': 15, '캐시겉옷': 15,
  '캐시 목/어깨장식': 16, '캐시목/어깨장식': 16,
  '캐시 신발': 17, '캐시신발': 17,
  '캐시 망토': 18, '캐시망토': 18,
  '캐시 얼굴장식': 19, '캐시얼굴장식': 19,
  '캐시 장신구': 20, '캐시장신구': 20,
  '캐시 세트옷': 21, '캐시세트옷': 21,
  '캐시 방패/보조무기': 22, '캐시방패/보조무기': 22
};


const limit = pLimit(30);
const webLimit = pLimit(10); // 안정성을 위해 10으로 하향

// 재시도 로직을 포함한 axios 래퍼
async function fetchWithRetry(url, params = {}, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      return await axios.get(url, { params, timeout: 8000 });
    } catch (err) {
      if (i === retries - 1) throw err;
      await new Promise(r => setTimeout(r, 1000 * (i + 1))); // 지수 백오프
    }
  }
}


async function getOcid(characterName, serverName) {
  try {
    const resp = await axios.get('https://open.api.nexon.com/baram/v1/id', {
      params: { character_name: characterName, server_name: serverName },
      headers: { 'x-nxopen-api-key': NEXON_API_KEY }
    });
    return resp.data.ocid;
  } catch { return null; }
}

async function processCharacter(characterName, serverName, dbServerId, jobCode) {
  try {
    const ocid = await getOcid(characterName, serverName);
    if (!ocid) return;

    const [basicResp, equipResp] = await Promise.all([
      axios.get('https://open.api.nexon.com/baram/v1/character/basic', { params: { ocid }, headers: { 'x-nxopen-api-key': NEXON_API_KEY } }),
      axios.get('https://open.api.nexon.com/baram/v1/character/item-equipment', { params: { ocid }, headers: { 'x-nxopen-api-key': NEXON_API_KEY } })
    ]);

    const PET_NAMES = ["주작", "현무", "백호", "청룡", "황룡", "혼돈", "도올", "궁기", "도철", "고대불의", "고대바람의", "고대땅의", "고대물의", "생명의목걸이"];
    // 1. 공백 제거 및 안전한 매핑
    const itemsToProcessRaw = (equipResp.data.item_equipment || [])
      .filter(i => i.item_id)
      .map(i => {
        // 앞뒤 공백을 제거하여 PART_MAP 적중률을 높임
        const slotName = (i.item_equipment_slot_name || '').trim();
        return {
          name: i.item_id.trim(),
          part_id: PART_MAP[slotName] || 23
        };
      })
      .filter(item => {
        const hasPrefix = PET_NAMES.some(prefix => item.name.startsWith(prefix));
        const hasSuffix = /\d+성$/.test(item.name);
        return !(hasPrefix && hasSuffix);
      });

    const partCounts = {};
    const itemsToProcess = [];

    // 2. 23번 예외 처리 및 한도 로직 강화
    for (const item of itemsToProcessRaw) {
      // 23번(미분류/기타) 부위는 개수 제한을 두지 않고 무조건 살립니다.
      if (item.part_id !== 23) {
        const limit = (item.part_id === 4 || item.part_id === 9) ? 2 : 1;
        partCounts[item.part_id] = (partCounts[item.part_id] || 0) + 1;

        // 허용 개수를 초과한 장비(프리셋 등)는 배열에 담지 않고 버림
        if (partCounts[item.part_id] > limit) {
          continue;
        }
      }
      itemsToProcess.push(item);
    }

    if (itemsToProcess.length === 0) {
      process.stdout.write(characterName + 's'); // s for skipped
      return;
    }

    // 데드락 방지를 위한 다중 정렬 (1순위: part_id 오름차순, 2순위: name 가나다순)
    itemsToProcess.sort((a, b) => {
      if (a.part_id !== b.part_id) return a.part_id - b.part_id;
      return a.name.localeCompare(b.name, 'ko');
    });

    const genderStr = basicResp.data.character_gender;
    const genderCode = genderStr === 'M' ? 1 : (genderStr === 'F' ? 2 : null);

    return {
      server_id: dbServerId,
      character_name: characterName,
      job_id: jobCode,
      gender: genderCode,
      level: basicResp.data.character_level,
      equipment_json: itemsToProcess
    };
  } catch {
    return null;
  }
}

async function findLastPage(serverCode, jobCode) {
  let low = 0, high = 4999, lastGoodPage = -1;
  while (low <= high) {
    let mid = Math.floor((low + high) / 2);
    const startRank = (mid * 20) + 1;
    const url = `https://baram.nexon.com/Rank/List?maskGameCode=${serverCode}&n4Rank_start=${startRank}&codeGameJob=${jobCode}`;
    try {
      const resp = await fetchWithRetry(url);
      const $ = cheerio.load(resp.data);
      const firstCharPromotion = parseInt($('tr:nth-child(2) td:nth-child(6)').text()) || 0;
      const hasCharacters = $('tr').length > 1;

      if (hasCharacters && firstCharPromotion >= MIN_PROMOTION_LEVEL) {
        lastGoodPage = mid;
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    } catch { high = mid - 1; }
  }
  return lastGoodPage;
}

async function fetchCharacterNamesFromWeb(serverCode, jobCode) {
  const names = [];
  try {
    const lastPage = await findLastPage(serverCode, jobCode);
    if (lastPage === -1) return [];

    console.log(`    [*] 0 ~ ${lastPage} 페이지 병렬 수집 중...`);
    const pages = Array.from({ length: lastPage + 1 }, (_, i) => i);

    await Promise.all(pages.map(page => webLimit(async () => {
      const startRank = (page * 20) + 1;
      const url = `https://baram.nexon.com/Rank/List?maskGameCode=${serverCode}&n4Rank_start=${startRank}&codeGameJob=${jobCode}`;
      const response = await fetchWithRetry(url);
      const $ = cheerio.load(response.data);

      $('tr').each((_, el) => {
        const name = $(el).find('td:nth-child(3)').text().trim();
        const promotion = parseInt($(el).find('td:nth-child(6)').text()) || 0;
        if (name && promotion >= MIN_PROMOTION_LEVEL && promotion <= MAX_PROMOTION_LEVEL) names.push(name);
      });
    })));
  } catch (err) {
    console.error(`\n❌ 웹 크롤링 최종 실패: ${err.message}`);
  }
  return [...new Set(names)];
}

async function cleanupOldData() {
  console.log('\n[*] 2일 이상 경과된 오래된 데이터 정리 중...');
  const twoDaysAgo = new Date(Date.now() - 2 * 24 * 60 * 60 * 1000).toISOString();
  const { count, error } = await supabase.from('users').delete({ count: 'exact' }).lt('updated_at', twoDaysAgo);
  if (error) console.error('❌ 데이터 정리 실패:', error.message);
  else console.log(`[*] 정리 완료: ${count || 0}명의 캐릭터 삭제됨`);
}

async function runPipeline() {
  const args = process.argv.slice(2);
  const targetJobArg = args.find(a => a.startsWith('--job='))?.split('=')[1];
  const targetServerArg = args.find(a => a.startsWith('--server='))?.split('=')[1];
  const targetJob = targetJobArg ? parseInt(targetJobArg) : null;
  const targetServer = targetServerArg || null;
  const start = new Date().getTime();
  console.log(`>>> 유저 경계탐색 파이프라인 가동: ${MIN_PROMOTION_LEVEL}차 ~ ${MAX_PROMOTION_LEVEL}차`, start);

  for (const [serverName, nexonServerCode] of Object.entries(NEXON_SERVERS)) {
    if (targetServer && serverName !== targetServer) continue;
    for (const jobCode of JOBS) {
      if (targetJob !== null && jobCode !== targetJob) continue;
      const dbServerId = DB_SERVER_IDS[serverName];
      console.log(`\n[*] 수집 중: ${serverName} (직업: ${jobCode})`);
      const characterNames = await fetchCharacterNamesFromWeb(nexonServerCode, jobCode);
      console.log(`    -> ${characterNames.length}명의 캐릭터명 수집됨`);

      const BATCH_SIZE = 100;
      for (let i = 0; i < characterNames.length; i += BATCH_SIZE) {
        const chunk = characterNames.slice(i, i + BATCH_SIZE);
        const results = await Promise.all(chunk.map(name => limit(() => processCharacter(name, serverName, dbServerId, jobCode))));
        const validResults = results.filter(Boolean);

        if (validResults.length > 0) {
          const { error } = await supabase.rpc('upsert_character_data_batch', {
            p_characters: validResults
          });

          if (error) {
            console.error(`\n❌ 배치 RPC 저장 실패 (${i} ~ ${i + BATCH_SIZE}):`, error.message);
          } else {
            process.stdout.write(`[${validResults.length}명 저장] `);
          }
        }
      }
    }
  }

  if (targetJob === null && targetServer === null) {
    await cleanupOldData();
  }
  console.log('\n>>> 파이프라인 완료', new Date().getTime() - start, 'ms');
}

runPipeline().catch(console.error);
