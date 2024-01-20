unit UnitMarket;

interface

uses
  System.Generics.Collections,
  System.Threading, LoggerPro.GlobalLogger, Redis.Commons, Redis.Client, Redis.Values, Redis.NetLib.Indy;

type
  TSide = (Buy, Sell);

  TOrder = class
  public
    CreatorID: string;
    Side: TSide;
    Quantity: double;
    Price: double;

    constructor Create(id:string);  overload;
  end;

  TMatch = class
  public
    Bid: TOrder;
    Offer: TOrder;
  end;

  TMarket = class
  private
    FBids: TObjectList<TOrder>;
    FOffers: TObjectList<TOrder>;
    FMatches: TObjectList<TMatch>;
  public
    constructor Create;
    destructor Destroy; override;

    procedure AddOrder(order: TOrder);
    procedure MatchOrders;
    function ComputeClearingPrice: Integer;

    property Bids: TObjectList<TOrder> read FBids;
    property Offers: TObjectList<TOrder> read FOffers;
    property Matches: TObjectList<TMatch> read FMatches;
  end;

  const
   XMin = 0;
   XMax = 1000000000;

implementation

uses
  System.SysUtils, MainFormU, StrUtils;

{ TMarket }

constructor TMarket.Create;
begin
  FBids := TObjectList<TOrder>.Create;
  FOffers := TObjectList<TOrder>.Create;
  FMatches := TObjectList<TMatch>.Create;
end;

destructor TMarket.Destroy;
begin
  FBids.Free;
  FOffers.Free;
  FMatches.Free;
  inherited;
end;

procedure TMarket.AddOrder(order: TOrder);
begin
  if order.Side = TSide.Sell then
    FOffers.Add(order)
  else
    FBids.Add(order);
end;

procedure Log(s:string);
begin
  MainForm.Memo1.Lines.Add(s);
end;



function StringToOrder(const orderString: string; side:TSide): TOrder;
var
  orderFields: TArray<string>;
begin
  orderFields := orderString.Split([',']);

  var lRedis := NewRedisClient();
   var  lCmd := NewRedisCommand('XRANGE');
    lCmd.Add('order_history:XH2USD');
    lCmd.Add(orderString);    lCmd.Add(orderString);

   var lRes: TRedisRESPArray := lRedis.ExecuteAndGetRESPArray(lCmd);
    if Assigned(lRes) then begin
     Log(lRes.ToJSON());

    end;

  lRedis.Disconnect();


//    if Length(orderFields) = 4 then
//  begin
   var lSizeOfMyStreamArray := lRes.Items[0].ArrayValue.Items[1].ArrayValue.Count;
    Result := TOrder.Create;
    Result.CreatorID := lRes.Items[0].ArrayValue.Items[0].Value;
    Result.Side := side;
    Result.Quantity := lRes.Items[0].ArrayValue.Items[1].ArrayValue.Items[5].value.todouble;
//         var str := lRes.Items[0].ArrayValue.Items[1].ArrayValue.Items[7].value;
    Result.Price := lRes.Items[0].ArrayValue.Items[1].ArrayValue.Items[7].value.todouble; //ArrayValue.Items[1].ArrayValue[7].Value.ToDouble;
//  end
//  else
//    raise Exception.Create('Invalid order string format');
end;

procedure GetOrderDetailsFromStream(Order:TOrder);
begin
 if (Order.Quantity >0) and (order.Price >0) then exit;

// var
//  orderFields: TArray<string>;
//begin
//  orderFields := orderString.Split([',']);

  var lRedis := NewRedisClient();
   var  lCmd := NewRedisCommand('XRANGE')
                 .Add('order_history:XH2USD')
                 .Add(Order.CreatorID)
                 .Add(Order.CreatorID);

   var lRes: TRedisRESPArray := lRedis.ExecuteAndGetRESPArray(lCmd);
    if Assigned(lRes) then begin
     Log(lRes.ToJSON());

    end;

  lRedis.Disconnect();


//    if Length(orderFields) = 4 then
//  begin
   var lSizeOfMyStreamArray := lRes.Items[0].ArrayValue.Items[1].ArrayValue.Count;
//    Result.CreatorID := lRes.Items[0].ArrayValue.Items[0].Value;
    Order.Quantity := lRes.Items[0].ArrayValue.Items[1].ArrayValue.Items[5].value.todouble;
//         var str := lRes.Items[0].ArrayValue.Items[1].ArrayValue.Items[7].value;
    Order.Price := lRes.Items[0].ArrayValue.Items[1].ArrayValue.Items[7].value.todouble; //ArrayValue.Items[1].ArrayValue[7].Value.ToDouble;
end;

function OrderToString(const order: TOrder): string;
begin
  Result := Format('%d,%d,%d,%d', [order.CreatorID, Ord(order.Side), order.Quantity, order.Price]);
end;

procedure ReportNewMatch(Match:TMatch);
begin
  mainform.Memo1.Lines.Add('!!! New match: BID '+ Match.Bid.CreatorID+' ORDER'+Match.Offer.CreatorID);
end;


procedure TMarket.MatchOrders;
var
  newBid, newOffer: TOrder;
  bidsKey, offersKey: string;

  bids, offers: TRedisArray;
  LBids, LOffers: TList<TOrder>;
  I:integer;
begin
  bidsKey   := 'BIDS:XH2USD';
  offersKey := 'OFFERS:XH2USD';

  var RedisClient := NewRedisClient();
  // Retrieve bids and offers from Redis sorted sets


//  var
//  lCmd: IRedisCommand;
// var lRes: TRedisNullable<string>;
//  // ZADD BIDS:XH2USD 40000 1705351179675-0
//  lCmd := NewRedisCommand('ZRANGE').Add(bidsKey).Add('10000000000').Add('0').Add('BYSCORE').Add('REV').Add('LIMIT').Add('0').Add('1');
//  lRes := RedisClient.ExecuteWithStringResult(lCmd);




  bids   := RedisClient.ZRANGE(bidsKey, XMax, XMin, True, 1);
  offers := RedisClient.ZRANGE(offersKey, XMin, XMax, false, 1);

  LBids   := TList<TOrder>.Create;
  LOffers := TList<TOrder>.Create;

  for i := 0 to bids.count-1   do LBids.Add(TOrder.Create(Bids.Items[i]));
  for i := 0 to offers.count-1 do LOffers.Add(TOrder.Create(Offers.Items[i]));

  // Process bids and offers
  while (LBids.count > 0) and (LOffers.count > 0) do
  begin
    // Convert bid and offer strings to TOrder objects (you need to implement this)
    GetOrderDetailsFromStream(LBids[0]);
    GetOrderDetailsFromStream(LOffers[0]);

    newBid := nil;    newOffer := nil;
//    currBid := StringToOrder(bids.items[0],BUY);
//    currOffer := StringToOrder(offers.items[0],SELL);

    if LBids[0].Price < LOffers[0].Price then
      Break
    else
    begin
      if LBids[0].Quantity <> LOffers[0].Quantity then
      begin
        if LBids[0].Quantity > LOffers[0].Quantity then
        begin
          newBid := TOrder.Create;
          newBid.CreatorID := LBids[0].CreatorID;
//          newBid.Side := currBid.Side;
          newBid.Quantity := LBids[0].Quantity - LOffers[0].Quantity;
          newBid.Price := LBids[0].Price;

          LBids[0].Quantity := LOffers[0].Quantity;     //Kupi³em ile bu³o do sprzedania
        end
        else
        begin
          newOffer := TOrder.Create;
          newOffer.CreatorID := LOffers[0].CreatorID;
//          newOffer.Side := currOffer.Side;
          newOffer.Quantity := LOffers[0].Quantity - LBids[0].Quantity;
          newOffer.Price := LOffers[0].Price;

          LOffers[0].Quantity := LBids[0].Quantity;            // sprzeda³em ile by³o do kupienia
        end;
      end;
    end;

//    FMatches.Add(TMatch.Create);
//    FMatches.Last.Bid := LBids[0];
//    FMatches.Last.Offer := LOffers[0];
//    redisclient.SADD(FMatches.Last.Bid.CreatorID,Fmatches.Last.Offer.CreatorID);
//    ReportNewMatch(FMatches.Last);
//
//    LBids.Delete(0);
//    RedisClient.ZREM(bidsKey, LBids[0].CreatorID);
//    LOffers.Delete(0);
//    RedisClient.ZREM(offersKey, LOffers[0].CreatorID);
//
//    if assigned(newBid) then begin
//    LBids.insert(0,newBid);
//    NewOrderConsumer1(newBid.CreatorID,'BUY',newBid.Quantity.ToString,newBid.Price.ToString);
//    end;
//    if assigned(newOffer) then begin
//    LBids.insert(0,newoffer);
//    NewOrderConsumer1(newoffer.CreatorID,'SELL',newoffer.Quantity.ToString,newoffer.Price.ToString);
//    end;
  end;

  LBids.Free;
  LOffers.Free;
end;


function TMarket.ComputeClearingPrice: Integer;
var
  Match: TMatch;
  CumulativeQuantity, ClearingPrice: double;
begin
  if FMatches.Count = 0 then
    Exit(0);

  ClearingPrice := 0;
  CumulativeQuantity := 0;

  for Match in FMatches do
  begin
    CumulativeQuantity := CumulativeQuantity + Match.Bid.Quantity;
    ClearingPrice := ClearingPrice + Match.Bid.Quantity * ((Match.Bid.Price + Match.Offer.Price) / 2);
  end;

  Result := Round(ClearingPrice / CumulativeQuantity);
end;

{ TOrder }

constructor TOrder.Create(id: string);
begin
 CreatorID := id;
end;

end.

