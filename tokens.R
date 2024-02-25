# -*- coding: UTF-8 -*-
library(tidyverse)
library(data.table)
library(RcppMeCab)
library(KoNLP)
library(foreach)
library(doParallel)
library(furrr)
library(progressr)


rm(list=ls())


### 1.파일 불러오기
file_path = "C:\\Users\\jeong\\token"
df <-fread(file.path(file_path,"highlight.csv"),encoding = "UTF-8")

### 2.전처리 
# a.하이라이트 뽑기
df <- df %>% mutate(num1=str_split_fixed(df$keywords,",",2)[,1],
                   num2=str_split_fixed(df$keywords,",",2)[,2],
                   highlight=substr(message,start=as.integer(num1)+1,stop=as.integer(num2)+1)) %>%
             select(brand_code, review_id, review_tag_type_id, highlight)


# b.전처리 하이라이트 리스트 정리/중복제거/na제거
re_list <- unique(df$highlight)
re_list <- re_list[!is.na(re_list)]
re_list <- re_list[re_list != ""]

# c.매칭용 데이터 프레임 만들기
num <- c(1:length(re_list))
col1 <- data.frame(num)
re_df  <- cbind(col1,re_list)

### 3.병렬 연산 
# a.준비
numCores <- detectCores() - 2
myCluster <- makeCluster(numCores)
registerDoParallel(myCluster)

# b.형태소 분리 펑션
f_result <- function(x){
  f_re <- RcppMeCab::posParallel(x,join=FALSE,format="data.frame")
  f_re$num <- which(re_list == x)
  return(f_re)
}

# c.병렬 진행
with_progress({
  p <- progressor(steps = 100)
  f_result_df <- 
  foreach(x = re_list, .combine = rbind) %dopar% {
    # p() 
    f_result(x) 
  }
})

# d.병렬 종료
stopCluster(myCluster)

### 4.전처리
# a.형태소 분리 결과 매칭용 테이블에 합치기
re_df <-
  re_df %>% left_join(f_result_df %>% mutate_at(vars(num),as.integer), by ="num")

# b.불필요한 형태소 제거
re_df <- 
  re_df %>%  mutate(nn=grepl("\n|\r",token),nnn=grepl("[^ \uAC00-\uD7A30a-zA-Z]",token)) %>% 
  filter(nn == FALSE & nnn == FALSE) %>%   select(-nn,-nnn,-subtype) %>% rename(highlight=re_list)

# c.하이라이트 테이블에 형태소 붙이기
df <- 
  as.data.table(re_df)[as.data.table(df), on = .(highlight), allow.cartesian=TRUE] %>% 
  select(doc_id,sentence_id,token_id,token,pos,review_id,review_tag_type_id,brand_code,highlight)
  
# d.명사 동사 형용사 선택
ok_pos <- c("^NN|^VA|^VV|^VX|^XR|MAG" )
not_pos <- c("^NNB")

df <- df[!is.na(df$pos),]

df <- df[grepl(ok_pos, df$pos),]

df <- df[!grepl(  not_pos, df$pos),]

# df$token <- iconv(as.character(df$token),from = "EUC-kr")














# file_path <- "C:\\Users\\jeong\\langchain\\output"
# df <- fread(file.path(file_path,"tokens.csv"),encoding="UTF-8")
# review <- fread(file.path(file_path,"review_tags.csv"),encoding="UTF-8") %>%
#   mutate(ccode = paste0(brand_code,review_id,review_tag_type_id)) %>% select(ccode) %>% unique() %>% unlist(use.names = FALSE)

df <- df %>% mutate_at(vars(review_id,review_tag_type_id),as.character) %>% arrange(brand_code,review_id,review_tag_type_id,doc_id,token_id)

tlist <- "짧다|짧|달라붙다|달라붙|크다|크|작다|작|두껍|도톰|두툼|얇|늘어나|늘어난다|늘어남|늘어짐|늘어납니다"
df_tlist <- df[grepl(tlist, df$token),] %>% select(brand_code,review_id,review_tag_type_id) %>%
  mutate(ccode = paste0(brand_code,review_id,review_tag_type_id)) %>% select(ccode) %>% unique() %>% unlist(use.names = FALSE)
df_anh_list <- df[grepl("^않|안", df$token  ),] %>% select(brand_code,review_id,review_tag_type_id) %>%
  mutate(ccode = paste0(brand_code,review_id,review_tag_type_id)) %>% select(ccode) %>% unique() %>% unlist(use.names = FALSE)

df_anh <- df %>% mutate(ccode = paste0(brand_code,review_id,review_tag_type_id)) %>% 
  filter(ccode %in% df_tlist) %>%  filter(ccode %in% df_anh_list)     # %>%  filter(ccode %in% review)










# # 쉬프트 적용
# shift1 <- df_anh$token %>% unlist(use.names = FALSE)
# shift1 <- c(tail(shift1, -1), head(shift1, 1)) 
# shift2 <- df_anh$token %>% unlist(use.names = FALSE)
# shift2 <- c(tail(shift2, -2), head(shift2, 2)) 
# 
# df_anh$shift1 <- shift1
# df_anh$shift2 <- shift2
# 
# df_anh <- df_anh %>% mutate(token2 = paste(token,shift1),
#                             token3 = paste(token,shift1,shift2))
# 
# df_anh$new_token = ""
# ### 병렬 연산 설정
# # a.병렬 코어 선택
# 
# availableCores()
# plan(multisession, workers = 10)
# 
# # b.메모리 최대 사용량 설정
# 
# options(future.globals.maxSize = 16000 * 1024^2) 
# 
# future_map(1:length(df_anh$review_id), function(j){
#   df_anh$new_token[j] = 
#   if (grepl("^VA|^VV",df_anh$pos[j]) && grepl("않",df_anh$shift1[j]) || grepl("^안$",df_anh$token[j]))
#     {df_anh$shift1[j]}
#   else
#     {df_anh$token[j]}
# })
# 
# grep(" ",df_anh$new_token)














# 부정어 표기
aaa <- df_anh %>% group_by(brand_code,review_id,review_tag_type_id) %>% 
  summarise(pos2 = paste(pos,collapse = ","),token2 = paste(token,collapse = ","))

rm(list=ls()[grep("^df$",ls())])
### 병렬 연산 설정
# a.병렬 코어 선택

availableCores()
plan(multisession, workers = 10)

# b.메모리 최대 사용량 설정

options(future.globals.maxSize = 16000 * 1024^2) 


c=list()

c <-  future_map(1:length(aaa$review_id), function(j){# for (j in 1:length(aaa$review_id)){
  a=""
  b=""
  k=""
  pos = unlist(strsplit(aaa$pos2[j],","))
  token = unlist(strsplit(aaa$token2[j],","))
  len = length(pos)
  for (i in 1:len){
    a=ifelse(grepl("^VA|^VV",pos[i]),i,0)
    k=ifelse(grepl("^NN",pos[i]),i,0)
    b[i]=
      if (a > 0 && a < len  && token[a+1] == "않"){
        
        if (a>2 && token[a-1] == "크" && token[a] == "작"){
        "적당하다"
        }else if(a>2 && token[a-1] == "길" && token[a] == "짧"){
        "적당하다"  
        }else if(a>2 && token[a-1] == "작" && token[a] == "크"){
          "적당하다"
        }else if(a>2 && token[a-1] == "짧" && token[a] == "길"){
          "적당하다"  
        }else  { paste0(token[a],"지않다")}
        
        
      } else if (i > 1 && token[a-1] == "안"){
        paste0("안",token[a],"다")

      } else{ token[i] }
  }
  c[[j]] = c(b)
})

  

data.list.one <- lapply(c, paste, collapse=" ")
data.list.df <- as.data.frame(do.call("rbind", data.list.one))


max_cols <- max(sapply(strsplit(data.list.df$V1, " "), length))

# 빈 데이터프레임 생성
new_df <- data.frame(matrix("", nrow = nrow(data.list.df), ncol = max_cols))

# 컬럼 이름 설정
colnames(new_df) <- paste0("c", 1:max_cols)

# 문자열을 " "로 분해하고 새로운 데이터프레임에 저장
split_values <- strsplit(data.list.df$V1, " ")

for (i in 1:nrow(data.list.df )) {
  new_df[i, 1:length(split_values[[i]])] <- split_values[[i]]
}


### 병렬 초기화

gc()
options(future.fork.multithreading.enable = FALSE)

## 했으면 좋겠다 추가 





# 원래 데이터프레임과 새로운 데이터프레임을 합치기
result_df <- cbind(aaa, new_df)
result_df <- result_df %>% select(-pos2,-token2)

# 새로로 확장
col_len　<- dim(result_df)[2]
final_df <- result_df %>% melt(id.vars = c(1:3),measure.vars = c(4:col_len)) %>% arrange(brand_code,review_id,review_tag_type_id,variable) 
final_df <- final_df[final_df$value!="",-4 ] 

# pos 추가
final_df <- cbind(final_df,df_anh$pos)
final_df <- final_df %>% rename(token = value, pos = 'df_anh$pos')

## 안 않 적당하다 정리


# 합치기
# final_df <- fread(file.path(file_path,"tokens.csv"),encoding="UTF-8")
no_anh_df <- final_df%>% mutate(ccode = paste0(brand_code,review_id,review_tag_type_id)) %>%
  anti_join(df_anh, by = 'ccode') %>%
  select(brand_code,review_id,review_tag_type_id,token,pos)
ffinal_df <- rbind(final_df,no_anh_df)

# 미노출
non_list <- 
"^심합니다$|^티나$|^그러지않다$|^들어감$|^했으나$|^줘야$|^엄청나$|^어떨지$|^할까$|^졌어요$|^그래요$|^봐도$|^가요$|^보단$|^사니$|^구하$|^든다$|^니다$|^못했$|^돼서$|^나옴$|^나와요$|^사요$|^입혀요$|^줘야$|^와요$|^어쩌$|^봐서$|^는데$|^샀으면$|^바랍니다$|^사려$|^져요$|^못하$|^보임$|^삽니다$|^봅니다$|^려고$|^위해$|^든다고$|^원했$|^그런가$|^사도$|^고요$|^갑니다$|^좋아하$|^구요$|^사면$|^봐요$|^좋아합니다$|^다닙니다$|^다녀요$|^살까$|^나갈$|^입혔$|^최고$|^부분$|^괜찮$|^보여$|^정도$|^행복$|^느낌$|^보이$|^감사$|^인가$|^아쉬움$|^보인$|^추천$|^가능$|^아쉬워$|^보여요$|^이번$|^덕분$|^필요$|^줘서$|^기본$|^처음$|^마음$|^이용$|^사고$|^적당$|^완전$|^사이$|^평소$|^강추$|^사람$|^걱정$|^다음$|^바보$|^아이템$|^내요$|^물건$|^자체$|^기분$|^적용$|^만족$|^비해$|^아쉽$|^해야$|^아쉬운$|^내년$|^귀찮$|^아프$|^비슷$|^나가$|^지나$|^속상했$|^아깝$|^그랬$|^들어가$|^속상하$|^그렇$|^보낼$|^보여서$|^아쉬웠$|^버릴$|^원일$|^져서$|^다르$|^도착$|^최고$|^걸렸$|^동일$|^추가$|^기대$|^놀랐$|^완전$|^다행$|^똑같$|^걸리$|^걸린$|^걸려서$|^걸림$|^이해$|^급하$|^드려요$|^맞음$|^걸릴$|^시켰$|^입혀야$|^선택$|^참고$|^생기$|^빠지$|^힘들$|^어쩔$|^대신$|^납니다$|^당황$|^한철$|^차이$|^의사$|^써서$|^다닐$|^잘사$|^버리$|^버렸$|^그러$|^변형$|^보냈$|^스러워$|^빈다$|^시중$|^나와$|^보낸$|^걸려요$|^숩니다$|^걸려$|^마니$|^칩니다$|^대신$|^할지$|^가능$|^느껴$|^좋아합니$|^길이$|^해도$|^해요$|^대비$|^주문$|^봐야$|^써야$|^줘요$|^디자인$|^기능성$|^왔어요$|^됩니다$|^색상$|^하여$|^한데$|^이즈$|^구입$|^만들$|^와서$|^신축성$|^합니다$|^이정도$|^돌려도$|^소재$|^하게$|^나왔$|^싸이$|^생각$|^보내$|^시킬$|^마감처리$|^했어요$|^마세요$|^그럼에도$|^배송$|^제품$|^보이$|^나오$|^이상$|^판매$|^조아$|^착용감$|^샀어요$|^드립니다$|^겠어요$|^가격$|^상품$|^입힐$|^입혀$|^격도$|^돌리$|^드렸$|^사이즈$|^나와서$|^사세요$|^그런지$|^해서$|^구매$|^입히$|^리뷰$|^다니$|^돌렸$|^나온$|^스타일$|^그래도$|^보입니다$|^아이$|^심하$|^봐야$|^입기$|^모르$"

non <- ffinal_df[!grepl(non_list, ffinal_df$token  ),]
non <- non %>% mutate(len = nchar(token))

one_list <-  "^크$|^작$|^길$|^짧$|^늦$|^얇$"

one <- non[grepl(one_list, non$token),]
one$token <- iconv(as.character(one$token),from = "utf-8")

two <- non[nchar(non$token)>1,]
two$token <- iconv(as.character(two$token),from = "utf-8")

final <- rbind(one,two)
final <- final[,1:5]



mapping = c("편하"="편하다","성비"="가성비","커서"="크다","착하"="착하다","이뻐"="예쁘다","마르"="마르다","비싸"="비싸다","예쁠"="예쁘다","예쁜데"="예쁘다",
            "따숩"="따뜻하다","올라"="올랐다","편한"="편하다","거칠"="까칠하다","줄지"="줄어든다","이쁨"="예쁘다","크"="크다","커도"="크다","추워져서"="춥다",
            "편해요"="편하다","작아서"="작다","어울리"="어울리다","괜찮아요"="괜찮다","예쁩니다"="예쁘다","부드러워"="부드럽다","드럽"="더럽다",
            "빨라요"="빠르다","기다리"="기다리다","이즈라"="사이즈", "두꺼워서"="두껍다","어두운"="어둡다","따가워"="까칠하다","더워서"="덥다","큰데"="크다",
            "약해서"="약하다","흘러내리"="흘러내리다","줄어드"="줄어든다","예쁘"="예쁘다","신축"="신축성","느렸"="느리다","더운"="덥다","더워"="덥다",
            "편함"="편하다","이쁜"="예쁘다","구겨"="구겨지다","비싼"="비싸다","따스"="따뜻하다","내려"="내리다","편해"="편하다","추운"="춥다","추워"="춥다",
            "따갑"="까칠하다","빠짐"="빠진다","작"="작다","이뻐요"="예쁘다","느려서"="느리다","착해서"="착하다","편해서"="편하다","늦다"="느리다",
            "귀여워서"="귀엽다","어울려서"="어울리다","가볍워서"="가볍다","기다렸"="기다리다","부드러워서"="부드럽다","두꺼운"="두껍다","부드러우"="부드럽다",
            "어두워"="어둡다","까슬까슬"="까칠하다","늘어나"="늘어난다","내려요"="내리다","이쁘"="예쁘다","빠르"="빠르다","느리"="느리다","따듯"="따뜻하다",
            "편할"="편하다","오가"="오가닉","빨랐"="빠르다","내렸"="내리다","따시"="따뜻하다","걸치"="걸치다","흘러"="흘러내리다","보드라워"="부드럽다",
            "거친"="까칠하다","질만"="품질","길"="길다","예뻐요"="예쁘다","빠릅니다"="빠르다","빨라서"="빠르다","부드럽"="부드럽다","보드랍"="부드럽다",
            "귀여운"="귀엽다","어울릴"="어울리다","가벼워"="가볍다","기다린"="기다리다","가벼운"="가볍다","두꺼워"="두껍다","나쁘"="나쁘다",
            "받쳐입"="받쳐입다","까슬한"="까칠하다","늘어남"="늘어난다","올라가"="올라간다","가성"="가성비","커요"="크다","느린"="느리다",
            "편안"="편하다","일가"="세일가","가볍"="가볍다","귀엽"="귀엽다","두껍"="두껍다","받쳐"="받쳐입다","까칠"="까칠하다","비치"="비치다","비침"="비치다",
            "쪼이"="조이다","생길"="생기다","짧"="짧다","큽니다"="크다","울립니다"="어울리다","편합니다"="편하다","이뻐서"="예쁘다","겨드랑"="겨드랑이",
            "구겨져서"="구겨지다","느려요"="느리다","부드러운"="부드럽다","기다릴"="기다리다","묻어나"="묻어난다","올라서"="올랐다","비쳐서"="비치다",
            "어려울"="어렵다", "흘러내려"="흘러내리다","늘어납니다"="늘어난다","줄어들"="줄어든다","정사"="정사이즈","빠른"="빠르다","에어리"="에어리즘",
            "괜찮"="괜찮다","인가"="할인가","예쁜"="예쁘다","오르"="올랐다","무겁"="무겁다","어렵"="어렵다","조여"="조이다","늦"="늦다",
            "작아요"="작다","어울려요"="어울리다","떨어지"="떨어지다","예뻐서"="예쁘다","내려갔"="내리다","내려서"="내리다","귀여워"="귀엽다",
            "기다려서"="기다리다","가벼워서"="가볍다","무거운"="무겁다","무거움"="무겁다","까슬하"="까칠하다","늘어짐"="늘어난다","립니"="느리다",
            "올랐"="올랐다","비갑"="가성비","조이"="조이다","어둡"="어둡다","까슬"="까칠하다","말려"="말리다","얇"="얇다","달라붙"="달라붙다",
            "타이트한"="타이트","떨어져"="떨어지다","떨어졌"="떨어지다","떨어져서"="떨어지다","내려가"="내리다","구겨져"="구겨지다","무거워"="무겁다",
            "무거워서"="무겁다","일어나"="일어난다","내리"="내리다","말리"="말리다","구겨진"="구겨지다","줄이"="줄이다","줄였"="줄이다","줄일"="줄이다",
            "줄여야"="줄이다","빠름"="빠르다","빨라"="빠르다", "빨르"="빠르다","빨랐으면"="빠르다","느려"="느리다","늦다"="느리다","느림"="느리다",
            "색감"="색상","색깔"="색상","색도"="색상", "컬러"="색상","칼라"="색상","색이"="색상")



final$token <- ifelse(final$token %in% names(mapping), mapping[final$token], final$token)



### 5.결과 파일 저장
fwrite(final,file.path(file_path,"tokens3.csv"))






