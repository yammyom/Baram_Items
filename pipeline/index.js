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

// 목표 승급 단계 (7차 이상)
const TARGET_PROMOTION_LEVEL = 7;

const PART_MAP = {
  '무기': 1, '투구': 2, '갑옷': 3, '왼손': 4, '오른손': 4,
  '목장식': 5, '목/어깨장식': 5, '신발': 6, '망토': 7, '얼굴장식': 8,
  '보조1': 9, '보조2': 9, '보조': 9, '장신구': 10, '세트옷': 11, '방패/보조무기': 12,
  '캐시 무기': 13, '캐시 투구': 14, '캐시 겉옷': 15, '캐시 목장식': 16,
  '캐시 신발': 17, '캐시 망토': 18, '캐시 얼굴장식': 19, '캐시 장신구': 20,
  '캐시 세트옷': 21, '캐시 방패/보조무기': 22
};

const itemCache = new Map();
const inFlightRequests = new Map(); // 중복 DB 등록 방지용 락
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

async function initItemCache() {
  console.log('[*] 아이템 캐시 로드 중...');
  const { data, error } = await supabase.from('items').select('item_id, name, part_id');
  if (error) return console.error('❌ 캐시 로드 실패:', error.message);
  data.forEach(item => itemCache.set(`${item.name}|${item.part_id}`, item.item_id));
  console.log(`[*] ${itemCache.size}개의 아이템 캐시 로드 완료`);
}

async function getOrCreateItemIds(items) {
  const ids = [];
  const itemsToInsert = [];
  const inFlightPromises = [];

  // 1. 캐시 및 진행 중인 요청 확인
  for (const item of items) {
    const key = `${item.name}|${item.part_id}`;
    if (itemCache.has(key)) {
      ids.push(itemCache.get(key));
    } else if (inFlightRequests.has(key)) {
      // 이미 DB에 등록 중이라면 대기 리스트에 추가
      inFlightPromises.push(inFlightRequests.get(key));
    } else {
      itemsToInsert.push(item);
    }
  }

  // 진행 중인 등록 작업이 있다면 동시 대기
  if (inFlightPromises.length > 0) {
    await Promise.all(inFlightPromises);
  }

  // 다른 요청에 의해 캐시에 들어왔는지 재확인
  const realItemsToInsert = [];
  for (const item of itemsToInsert) {
    const key = `${item.name}|${item.part_id}`;
    if (itemCache.has(key)) {
      ids.push(itemCache.get(key));
    } else {
      realItemsToInsert.push(item);
    }
  }

  // 2. 캐시에 없는 새 아이템들을 DB에 등록
  if (realItemsToInsert.length > 0) {
    const uniqueItems = new Map();
    for (const item of realItemsToInsert) {
      uniqueItems.set(`${item.name}|${item.part_id}`, item);
    }

    const insertPromise = (async () => {
      try {
        const { data, error } = await supabase.from('items')
          .upsert(Array.from(uniqueItems.values()), { onConflict: 'name, part_id' })
          .select();

        if (!error && data) {
          data.forEach(item => itemCache.set(`${item.name}|${item.part_id}`, item.item_id));
        }
      } catch (err) {
        console.error('❌ 장비 DB 저장 중 에러 발생:', err.message);
      }
    })();

    // 동시성 요청들이 이 Promise를 대기할 수 있도록 등록
    for (const key of uniqueItems.keys()) {
      inFlightRequests.set(key, insertPromise);
    }

    await insertPromise;

    // 작업 종료 후 Map에서 제거
    for (const key of uniqueItems.keys()) {
      inFlightRequests.delete(key);
    }
  }

  // 3. 최종 할당된 ID 수집
  for (const item of items) {
    const key = `${item.name}|${item.part_id}`;
    if (itemCache.has(key)) {
      ids.push(itemCache.get(key));
    }
  }

  return [...new Set(ids)];
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

    const itemsToProcess = (equipResp.data.item_equipment || [])
      .filter(i => i.item_id)
      .map(i => ({ name: i.item_id, part_id: PART_MAP[i.item_equipment_slot_name] || 23 }));

    const itemIds = await getOrCreateItemIds(itemsToProcess);

    if (itemIds.length === 0) {
      process.stdout.write(characterName + 's'); // s for skipped
      return;
    }

    await supabase.from('users').upsert({
      server_id: dbServerId,
      character_name: characterName,
      job_id: jobCode,
      level: basicResp.data.character_level,
      equipment_ids: itemIds,
      updated_at: new Date().toISOString()
    }, { onConflict: 'server_id, character_name' });

    process.stdout.write('.');
  } catch { /* skip */ }
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

      if (hasCharacters && firstCharPromotion >= TARGET_PROMOTION_LEVEL) {
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
        if (name && promotion >= TARGET_PROMOTION_LEVEL) names.push(name);
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
  const { count, error } = await supabase.from('users').delete().lt('updated_at', twoDaysAgo);
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
  console.log('>>> 초고속 경계탐색 파이프라인 가동:', TARGET_PROMOTION_LEVEL, '차 까지', start);
  await initItemCache();

  for (const [serverName, nexonServerCode] of Object.entries(NEXON_SERVERS)) {
    if (targetServer && serverName !== targetServer) continue;
    for (const jobCode of JOBS) {
      if (targetJob !== null && jobCode !== targetJob) continue;
      const dbServerId = DB_SERVER_IDS[serverName];
      console.log(`\n[*] 수집 중: ${serverName} (직업: ${jobCode})`);
      const characterNames = await fetchCharacterNamesFromWeb(nexonServerCode, jobCode);
      console.log(`    -> ${characterNames.length}명의 캐릭터명 수집됨`);
      await Promise.all(characterNames.map(name => limit(() => processCharacter(name, serverName, dbServerId, jobCode))));
    }
  }

  if (targetJob === null && targetServer === null) {
    await cleanupOldData();
  }
  console.log('\n>>> 파이프라인 완료', new Date().getTime() - start, 'ms');
}

runPipeline().catch(console.error);