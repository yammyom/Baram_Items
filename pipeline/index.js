require('dotenv').config();
const axios = require('axios');
const cheerio = require('cheerio');
const { createClient } = require('@supabase/supabase-js');

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY);
const NEXON_API_KEY = process.env.NEXON_API_KEY;

const NEXON_SERVERS = { '연': 131073, '무휼': 131074, '유리': 131086, '하자': 131087, '호동': 131088, '진': 131089 };
const DB_SERVER_IDS = { '연': 1, '무휼': 2, '유리': 3, '하자': 4, '호동': 5, '진': 6 };
const JOBS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];

const TARGET_PROMOTION_LEVEL = 6;

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

async function initItemCache() {
  console.log('[*] 아이템 캐시 로드 중...');
  const { data, error } = await supabase.from('items').select('item_id, name, part_id');
  if (error) return console.error('❌ 캐시 로드 실패:', error.message);
  data.forEach(item => itemCache.set(`${item.name}|${item.part_id}`, item.item_id));
  console.log(`[*] ${itemCache.size}개의 아이템 캐시 로드 완료`);
}

async function getOrCreateItemIds(items) {
  const ids = [];
  for (const item of items) {
    const key = `${item.name}|${item.part_id}`;
    if (itemCache.has(key)) {
      ids.push(itemCache.get(key));
    } else {
      const { data, error } = await supabase.from('items')
        .upsert(item, { onConflict: 'name, part_id' })
        .select();
      if (!error && data?.[0]) {
        const newId = data[0].item_id;
        itemCache.set(key, newId);
        ids.push(newId);
      }
    }
  }
  return [...new Set(ids)];
}

async function processCharacter(name, serverName, dbServerId, jobCode) {
  try {
    const idResp = await axios.get('https://open.api.nexon.com/baram/v1/id', {
      params: { character_name: name, server_name: serverName },
      headers: { 'x-nxopen-api-key': NEXON_API_KEY },
      timeout: 10000
    });
    const ocid = idResp.data.ocid;

    const [basicResp, equipResp] = await Promise.all([
      axios.get('https://open.api.nexon.com/baram/v1/character/basic', { params: { ocid }, headers: { 'x-nxopen-api-key': NEXON_API_KEY } }),
      axios.get('https://open.api.nexon.com/baram/v1/character/item-equipment', { params: { ocid }, headers: { 'x-nxopen-api-key': NEXON_API_KEY } })
    ]);

    const items = (equipResp.data.item_equipment || [])
      .filter(i => i.item_id)
      .map(i => ({ name: i.item_id, part_id: PART_MAP[i.item_equipment_slot_name] || 23 }));

    const itemIds = await getOrCreateItemIds(items);

    await supabase.from('users').upsert({
      server_id: dbServerId,
      character_name: name,
      job_id: jobCode,
      level: basicResp.data.character_level,
      equipment_ids: itemIds,
      updated_at: new Date().toISOString()
    }, { onConflict: 'server_id, character_name' });

    process.stdout.write('.');
  } catch (err) {
    // 무시하고 다음 캐릭터로
  }
}

async function findLastPage(serverCode, jobCode) {
  let low = 0, high = 4999, lastGoodPage = -1;
  while (low <= high) {
    let mid = Math.floor((low + high) / 2);
    const startRank = (mid * 20) + 1;
    const url = `https://baram.nexon.com/Rank/List?maskGameCode=${serverCode}&n4Rank_start=${startRank}&codeGameJob=${jobCode}`;
    try {
      const resp = await axios.get(url, { timeout: 10000 });
      const $ = cheerio.load(resp.data);
      const firstPromotion = parseInt($('tr:nth-child(2) td:nth-child(6)').text()) || 0;
      if ($('tr').length > 1 && firstPromotion >= TARGET_PROMOTION_LEVEL) {
        lastGoodPage = mid;
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    } catch { high = mid - 1; }
  }
  return lastGoodPage;
}

async function runPipeline() {
  const args = process.argv.slice(2);
  const targetJob = args.find(a => a.startsWith('--job='))?.split('=')[1];
  const targetServer = args.find(a => a.startsWith('--server='))?.split('=')[1];

  console.log('>>> 순차 처리 모드 가동:', new Date().toISOString());
  await initItemCache();

  for (const [serverName, serverCode] of Object.entries(NEXON_SERVERS)) {
    if (targetServer && serverName !== targetServer) continue;
    for (const jobCode of JOBS) {
      if (targetJob && jobCode !== parseInt(targetJob)) continue;

      console.log(`\n[*] 수집 중: ${serverName} (직업: ${jobCode})`);
      const dbServerId = DB_SERVER_IDS[serverName];
      const lastPage = await findLastPage(serverCode, jobCode);
      if (lastPage === -1) continue;

      for (let page = 0; page <= lastPage; page++) {
        const startRank = (page * 20) + 1;
        const url = `https://baram.nexon.com/Rank/List?maskGameCode=${serverCode}&n4Rank_start=${startRank}&codeGameJob=${jobCode}`;
        try {
          const resp = await axios.get(url, { timeout: 10000 });
          const $ = cheerio.load(resp.data);
          const names = [];
          $('tr').each((i, el) => {
            const name = $(el).find('td:nth-child(3)').text().trim();
            const promotion = parseInt($(el).find('td:nth-child(6)').text()) || 0;
            if (name && promotion >= TARGET_PROMOTION_LEVEL) names.push(name);
          });

          for (const name of names) {
            await processCharacter(name, serverName, dbServerId, jobCode);
          }
        } catch (err) {
          console.error(`\n❌ 페이지 ${page} 로드 실패: ${err.message}`);
        }
      }
    }
  }

  console.log('\n>>> 파이프라인 완료');
}

runPipeline().catch(console.error);
