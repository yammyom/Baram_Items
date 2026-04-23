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

const TARGET_PROMOTION_LEVEL = 6;
const BATCH_SIZE = 50; // 한 번에 저장할 캐릭터 수

const PART_MAP = {
  '무기': 1, '투구': 2, '갑옷': 3, '왼손': 4, '오른손': 4,
  '목장식': 5, '목/어깨장식': 5, '신발': 6, '망토': 7, '얼굴장식': 8,
  '보조1': 9, '보조2': 9, '보조': 9, '장신구': 10, '세트옷': 11, '방패/보조무기': 12,
  '캐시 무기': 13, '캐시 투구': 14, '캐시 겉옷': 15,
  '캐시 목장식': 16, '캐시 목/어깨장식': 16, '캐시 신발': 17,
  '캐시 망토': 18, '캐시 얼굴장식': 19, '캐시 장신구': 20, '캐시 세트옷': 21,
  '캐시 방패/보조무기': 22
};

const itemCache = new Map();
const limit = pLimit(30);
const webLimit = pLimit(10);

async function fetchWithRetry(url, params = {}, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      return await axios.get(url, { params, timeout: 10000 });
    } catch (err) {
      if (i === retries - 1) throw err;
      await new Promise(r => setTimeout(r, 2000 * (i + 1)));
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

// 아이템 ID 일괄 처리 (Batch)
async function getOrCreateItemIds(items) {
  const uniqueInThisCall = new Map();
  const resultIds = [];

  for (const item of items) {
    const key = `${item.name}|${item.part_id}`;
    if (itemCache.has(key)) {
      resultIds.push(itemCache.get(key));
    } else {
      uniqueInThisCall.set(key, item);
    }
  }

  if (uniqueInThisCall.size > 0) {
    const { data, error } = await supabase.from('items')
      .upsert(Array.from(uniqueInThisCall.values()), { onConflict: 'name, part_id' })
      .select();

    if (!error && data) {
      data.forEach(item => itemCache.set(`${item.name}|${item.part_id}`, item.item_id));
      // 다시 매핑
      return items.map(i => itemCache.get(`${i.name}|${i.part_id}`)).filter(id => id);
    }
  }
  return [...new Set(resultIds)];
}

async function getOcid(characterName, serverName) {
  try {
    const resp = await axios.get('https://open.api.nexon.com/baram/v1/id', {
      params: { character_name: characterName, server_name: serverName },
      headers: { 'x-nxopen-api-key': NEXON_API_KEY },
      timeout: 5000
    });
    return resp.data.ocid;
  } catch { return null; }
}

async function fetchCharacterData(name, serverName, dbServerId, jobCode) {
  try {
    const ocid = await getOcid(name, serverName);
    if (!ocid) return null;

    const [basicResp, equipResp] = await Promise.all([
      axios.get('https://open.api.nexon.com/baram/v1/character/basic', { params: { ocid }, headers: { 'x-nxopen-api-key': NEXON_API_KEY }, timeout: 5000 }),
      axios.get('https://open.api.nexon.com/baram/v1/character/item-equipment', { params: { ocid }, headers: { 'x-nxopen-api-key': NEXON_API_KEY }, timeout: 5000 })
    ]);

    const itemsToProcess = (equipResp.data.item_equipment || [])
      .filter(i => i.item_id)
      .map(i => ({ name: i.item_id, part_id: PART_MAP[i.item_equipment_slot_name] || 23 }));

    // 아이템 ID 획득
    const itemIds = await getOrCreateItemIds(itemsToProcess);

    return {
      server_id: dbServerId,
      character_name: name,
      job_id: jobCode,
      level: basicResp.data.character_level,
      equipment_ids: itemIds,
      updated_at: new Date().toISOString()
    };
  } catch { return null; }
}

async function runPipeline() {
  const args = process.argv.slice(2);
  const targetJobArg = args.find(a => a.startsWith('--job='))?.split('=')[1];
  const targetServerArg = args.find(a => a.startsWith('--server='))?.split('=')[1];
  const targetJob = targetJobArg ? parseInt(targetJobArg) : null;
  const targetServer = targetServerArg || null;

  console.log('>>> 배치 모드 파이프라인 가동:', new Date().toISOString());
  await initItemCache();

  for (const [serverName, nexonServerCode] of Object.entries(NEXON_SERVERS)) {
    if (targetServer && serverName !== targetServer) continue;
    for (const jobCode of JOBS) {
      if (targetJob !== null && jobCode !== targetJob) continue;

      const dbServerId = DB_SERVER_IDS[serverName];
      console.log(`\n[*] 수집 중: ${serverName} (직업: ${jobCode})`);

      const characterNames = await fetchCharacterNamesFromWeb(nexonServerCode, jobCode);
      console.log(`    -> ${characterNames.length}명의 캐릭터명 수집됨`);

      // 배치 처리 루프
      for (let i = 0; i < characterNames.length; i += BATCH_SIZE) {
        const batchNames = characterNames.slice(i, i + BATCH_SIZE);
        const userDataBatch = await Promise.all(
          batchNames.map(name => limit(() => fetchCharacterData(name, serverName, dbServerId, jobCode)))
        );

        const validUsers = userDataBatch.filter(u => u !== null);
        if (validUsers.length > 0) {
          const { error } = await supabase.from('users').upsert(validUsers, { onConflict: 'server_id, character_name' });
          if (error) console.error('\n❌ 배치 저장 실패:', error.message);
          else process.stdout.write(`[Batch ${i / BATCH_SIZE + 1} OK] `);
        }
      }
    }
  }

  if (targetJob === null && targetServer === null) {
    await cleanupOldData();
  }
  console.log('\n>>> 파이프라인 완료');
}

// 기존 findLastPage, fetchCharacterNamesFromWeb, cleanupOldData 함수 유지
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
      if ($('tr').length > 1 && firstCharPromotion >= TARGET_PROMOTION_LEVEL) {
        lastGoodPage = mid;
        low = mid + 1;
      } else { high = mid - 1; }
    } catch { high = mid - 1; }
  }
  return lastGoodPage;
}

async function fetchCharacterNamesFromWeb(serverCode, jobCode) {
  const names = [];
  try {
    const lastPage = await findLastPage(serverCode, jobCode);
    if (lastPage === -1) return [];
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
  } catch (err) { console.error(`\n❌ 웹 크롤링 실패: ${err.message}`); }
  return [...new Set(names)];
}

async function cleanupOldData() {
  console.log('\n[*] 오래된 데이터 정리 중...');
  const twoDaysAgo = new Date(Date.now() - 2 * 24 * 60 * 60 * 1000).toISOString();
  const { count, error } = await supabase.from('users').delete().lt('updated_at', twoDaysAgo);
  if (!error) console.log(`[*] 정리 완료: ${count || 0}명 삭제됨`);
}

runPipeline().catch(console.error);
