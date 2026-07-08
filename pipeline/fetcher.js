import pLimit from 'p-limit';
import { createClient } from '@supabase/supabase-js';

/**
 * Fetcher Worker (Cloudflare Queue Consumer)
 * - Queue에서 캐릭터명을 받아 넥슨 API를 호출하고 Supabase에 저장합니다.
 * - p-limit을 사용하여 초당 API 호출 수를 300회로 제한합니다.
 */

const limit = pLimit(300); // 넥슨 API 초당 300회 제한

const JOBS_MAP = {
  '전사': 1, '도적': 2, '주술사': 3, '도사': 4, '궁사': 5,
  '천인': 6, '마도사': 7, '영술사': 8, '차사': 9, '살수': 10, '흑화랑': 11
};

const PART_MAP = {
  '무기': 1, '투구': 2, '갑옷': 3, '왼손': 4, '오른손': 4,
  '목장식': 5, '목/어깨장식': 5, '신발': 6, '망토': 7, '얼굴장식': 8,
  '보조1': 9, '보조2': 9, '보조': 9, '장신구': 10, '세트옷': 11, '방패/보조무기': 12,
  '캐시 무기': 13, '캐시무기': 13,
  '캐시 투구': 14, '캐시투구': 14,
  '캐시 겉옷': 15, '캐시겉옷': 15,
  '캐시 목장식': 16, '캐시목장식': 16,
  '캐시 신발': 17, '캐시신발': 17,
  '캐시 망토': 18, '캐시망토': 18,
  '캐시 얼굴장식': 19, '캐시얼굴장식': 19,
  '캐시 장신구': 20, '캐시장신구': 20,
  '캐시 세트옷': 21, '캐시세트옷': 21,
  '캐시 방패/보조무기': 22, '캐시방패/보조무기': 22
};

export default {
  async queue(batch, env) {
    const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_ANON_KEY);
    const NEXON_API_KEY = env.NEXON_API_KEY;

    const tasks = batch.messages.map(msg =>
      limit(async () => {
        const { serverName, characterName } = msg.body;

        try {
          // 1. Get OCID
          const idResponse = await fetch(`https://open.api.nexon.com/baram/v1/id?character_name=${encodeURIComponent(characterName)}&server_name=${encodeURIComponent(serverName)}`, {
            headers: { 'x-nxopen-api-key': NEXON_API_KEY }
          });
          if (!idResponse.ok) return;
          const { ocid } = await idResponse.json();

          // 2. Get Basic Info (Level, Gender, Job)
          const basicResponse = await fetch(`https://open.api.nexon.com/baram/v1/character/basic?ocid=${ocid}`, {
            headers: { 'x-nxopen-api-key': NEXON_API_KEY }
          });
          const basicData = await basicResponse.json();
          const level = basicData.character_level;
          const genderStr = basicData.character_gender;
          const genderCode = genderStr === 'M' ? 1 : (genderStr === 'F' ? 2 : null);
          const jobCode = JOBS_MAP[basicData.character_class] || 1;

          // 3. Get Equipment Info
          const equipResponse = await fetch(`https://open.api.nexon.com/baram/v1/character/item-equipment?ocid=${ocid}`, {
            headers: { 'x-nxopen-api-key': NEXON_API_KEY }
          });
          const equipData = await equipResponse.json();

          // 장비 데이터 추출 및 items 테이블용 정규화
          const PET_NAMES = ["주작", "현무", "백호", "청룡", "황룡", "혼돈", "도올", "궁기", "도철", "고대불의", "고대바람의", "고대땅의", "고대물의", "생명의목걸이"];
          // 1. 공백 제거 및 안전한 매핑
          const itemsToProcessRaw = (equipData.item_equipment || [])
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
            msg.ack();
            return null;
          }

          // 데드락 방지를 위한 다중 정렬 (1순위: part_id 오름차순, 2순위: name 가나다순)
          itemsToProcess.sort((a, b) => {
            if (a.part_id !== b.part_id) return a.part_id - b.part_id;
            return a.name.localeCompare(b.name, 'ko');
          });

          const serverId = getServerId(serverName);

          return {
            msg,
            data: {
              server_id: serverId,
              character_name: characterName,
              job_id: jobCode,
              gender: genderCode,
              level: level,
              equipment_json: itemsToProcess
            }
          };
        } catch (err) {
          console.error(`Error fetching data for ${msg.body?.characterName}:`, err);
          return null; // 넥슨 API 에러 등은 null 반환하여 배치에서 제외 (nack 처리 여부는 고민해볼 수 있으나 일시적 에러일 수 있으므로 재시도하도록 놔두지 않고 일단 null 반환)
        }
      })
    );

    const results = await Promise.all(tasks);
    const validResults = results.filter(Boolean);

    if (validResults.length > 0) {
      // 동일 배치 내 중복 캐릭터 방어 (ON CONFLICT DO UPDATE는 같은 행을 두 번 건드리면 에러)
      const dedupMap = new Map();
      for (const r of validResults) {
        dedupMap.set(`${r.data.server_id}_${r.data.character_name}`, r);
      }
      const dedupedResults = [...dedupMap.values()];
      discardedMsgs.forEach(msg => msg.ack());
      const charactersData = dedupedResults.map(r => r.data);

      try {
        const { error } = await supabase.rpc('upsert_character_data_batch', {
          p_characters: charactersData
        });

        if (error) {
          console.error(`Batch RPC Error:`, error.message);
          throw error;
        }

        // 성공적으로 저장된 메시지들만 일괄 ack 처리
        dedupedResults.forEach(r => r.msg.ack());
        console.log(`[+] 성공적으로 ${dedupedResults.length}명 배치 저장 및 ACK 처리 완료.`);
      } catch (dbErr) {
        // 핸들러가 정상 반환되면 미ack 메시지도 성공 처리로 간주함
        // 따라서 명시적으로 retry()를 호출해야 실제로 큐가 재전송함
        console.error(`[-] Batch DB 처리 중 에러 발생, 재시도 예약:`, dbErr.message);
        dedupedResults.forEach(r => r.msg.retry({ delaySeconds: 5 }));
      }
    }
  }
};

// 서버명 -> 매핑 ID (1~6)
function getServerId(name) {
  const mapping = { '연': 1, '무휼': 2, '유리': 3, '하자': 4, '호동': 5, '진': 6 };
  return mapping[name] || 0;
}

